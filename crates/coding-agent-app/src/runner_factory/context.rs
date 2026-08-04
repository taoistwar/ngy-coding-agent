use std::any::Any;
use std::sync::Arc;

use coding_agent_provider::ChatCompletionsClient;
use coding_agent_runtime::ProcessLivenessScope;
use coding_agent_store::Store;
use uuid::Uuid;

use super::StartupRunnerFactoryError;
use crate::{PlatformPaths, RuntimeConfig, StoreWriterHandle, WallClock};

/// Immutable startup capabilities available before any production actor is
/// constructed. The absence of a StoreWriter is enforced by the type.
#[derive(Clone)]
pub struct PreActorStartupRunnerContext {
    paths: PlatformPaths,
    store: Store,
    wall_clock: Arc<dyn WallClock>,
    runtime_config: RuntimeConfig,
    instance_id: Uuid,
    process_liveness_scope: ProcessLivenessScope,
    validated_runner_inputs: Arc<dyn Any + Send + Sync>,
}

impl PreActorStartupRunnerContext {
    pub(crate) fn new(
        paths: PlatformPaths,
        store: Store,
        wall_clock: Arc<dyn WallClock>,
        validated_inputs: ValidatedStartupInputs,
        instance_id: Uuid,
        process_liveness_scope: ProcessLivenessScope,
    ) -> Self {
        Self {
            paths,
            store,
            wall_clock,
            runtime_config: validated_inputs.runtime_config,
            instance_id,
            process_liveness_scope,
            validated_runner_inputs: validated_inputs.runner_inputs,
        }
    }

    pub(crate) fn into_live(
        self,
        writer: StoreWriterHandle,
        prepared_runner_inputs: Arc<dyn Any + Send + Sync>,
    ) -> StartupRunnerContext {
        StartupRunnerContext {
            paths: self.paths,
            store: self.store,
            writer,
            wall_clock: self.wall_clock,
            runtime_config: self.runtime_config,
            instance_id: self.instance_id,
            process_liveness_scope: self.process_liveness_scope,
            validated_runner_inputs: self.validated_runner_inputs,
            prepared_runner_inputs,
        }
    }

    pub const fn paths(&self) -> &PlatformPaths {
        &self.paths
    }

    pub const fn store(&self) -> &Store {
        &self.store
    }

    pub const fn runtime_config(&self) -> &RuntimeConfig {
        &self.runtime_config
    }

    pub const fn process_liveness_scope(&self) -> &ProcessLivenessScope {
        &self.process_liveness_scope
    }
}

/// Live runner composition capabilities. Construction requires the one
/// production StoreWriter and an immutable pre-actor preparation result.
#[derive(Clone)]
pub struct StartupRunnerContext {
    paths: PlatformPaths,
    store: Store,
    writer: StoreWriterHandle,
    wall_clock: Arc<dyn WallClock>,
    runtime_config: RuntimeConfig,
    instance_id: Uuid,
    process_liveness_scope: ProcessLivenessScope,
    validated_runner_inputs: Arc<dyn Any + Send + Sync>,
    prepared_runner_inputs: Arc<dyn Any + Send + Sync>,
}

impl StartupRunnerContext {
    pub const fn paths(&self) -> &PlatformPaths {
        &self.paths
    }

    pub const fn store(&self) -> &Store {
        &self.store
    }

    pub const fn writer(&self) -> &StoreWriterHandle {
        &self.writer
    }

    pub fn wall_clock(&self) -> Arc<dyn WallClock> {
        Arc::clone(&self.wall_clock)
    }

    pub const fn runtime_config(&self) -> &RuntimeConfig {
        &self.runtime_config
    }

    pub const fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    pub const fn process_liveness_scope(&self) -> &ProcessLivenessScope {
        &self.process_liveness_scope
    }

    pub(super) fn production_provider(
        &self,
    ) -> Result<Arc<ChatCompletionsClient>, StartupRunnerFactoryError> {
        Arc::clone(&self.validated_runner_inputs)
            .downcast::<ChatCompletionsClient>()
            .map_err(|_| StartupRunnerFactoryError::new("RUNNER_PREFLIGHT_MISSING"))
    }

    pub(super) fn prepared<T>(&self) -> Result<Arc<T>, StartupRunnerFactoryError>
    where
        T: Any + Send + Sync,
    {
        Arc::clone(&self.prepared_runner_inputs)
            .downcast::<T>()
            .map_err(|_| StartupRunnerFactoryError::new("RUNNER_PREFLIGHT_MISSING"))
    }
}

pub(crate) struct ValidatedStartupInputs {
    runtime_config: RuntimeConfig,
    runner_inputs: Arc<dyn Any + Send + Sync>,
}

impl ValidatedStartupInputs {
    pub(crate) fn new(
        runtime_config: RuntimeConfig,
        runner_inputs: Arc<dyn Any + Send + Sync>,
    ) -> Self {
        Self {
            runtime_config,
            runner_inputs,
        }
    }
}
