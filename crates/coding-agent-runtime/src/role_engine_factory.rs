use std::sync::Arc;

use coding_agent_core::{
    ContextRedactor, PreparedModelProvider, ReviewDiffCheckpoint, Role, RoleEngine,
    RoleEngineFactory, RoleEventSink, RoleRun, RuntimeError,
};

use crate::{RoleScopedRuntime, RuntimeSession};

const FACTORY_SCOPE_MISMATCH: &str = "ROLE_ENGINE_FACTORY_SCOPE_MISMATCH";

/// Production role-engine factory over one task-owned provider/runtime session.
///
/// Every engine shares the exact provider, runtime session, event sink, and
/// redactor allocations. Only the independent runtime capability wrapper and
/// its Reviewer checkpoint authority vary by [`RoleRun`].
pub struct RoleScopedEngineFactory {
    provider: Arc<dyn PreparedModelProvider>,
    runtime_session: Arc<RuntimeSession>,
    events: Arc<dyn RoleEventSink>,
    redactor: Arc<dyn ContextRedactor>,
}

impl RoleScopedEngineFactory {
    pub fn new(
        provider: Arc<dyn PreparedModelProvider>,
        runtime_session: Arc<RuntimeSession>,
        events: Arc<dyn RoleEventSink>,
        redactor: Arc<dyn ContextRedactor>,
    ) -> Self {
        Self {
            provider,
            runtime_session,
            events,
            redactor,
        }
    }

    #[cfg(feature = "test-support")]
    pub fn shares_runtime_session_with(&self, session: &Arc<RuntimeSession>) -> bool {
        Arc::ptr_eq(&self.runtime_session, session)
    }

    #[cfg(feature = "test-support")]
    pub fn shares_provider_with(&self, provider: &Arc<dyn PreparedModelProvider>) -> bool {
        Arc::ptr_eq(&self.provider, provider)
    }

    #[cfg(feature = "test-support")]
    pub fn shares_event_sink_with(&self, events: &Arc<dyn RoleEventSink>) -> bool {
        Arc::ptr_eq(&self.events, events)
    }

    #[cfg(feature = "test-support")]
    pub fn shares_redactor_with(&self, redactor: &Arc<dyn ContextRedactor>) -> bool {
        Arc::ptr_eq(&self.redactor, redactor)
    }
}

impl RoleEngineFactory for RoleScopedEngineFactory {
    fn create_engine(
        &self,
        role_run: RoleRun,
        review_checkpoint: Option<ReviewDiffCheckpoint>,
    ) -> Result<RoleEngine, RuntimeError> {
        let runtime = match (role_run.role(), review_checkpoint) {
            (Role::Reviewer, Some(checkpoint)) => RoleScopedRuntime::try_with_review_checkpoint(
                role_run.role(),
                role_run.role_run(),
                Arc::clone(&self.runtime_session),
                Arc::clone(&self.redactor),
                checkpoint,
            )?,
            (Role::Planner | Role::Executor, None) => RoleScopedRuntime::try_new(
                role_run.role(),
                role_run.role_run(),
                Arc::clone(&self.runtime_session),
                Arc::clone(&self.redactor),
            )?,
            (Role::Reviewer, None) | (Role::Planner | Role::Executor, Some(_)) => {
                return Err(RuntimeError::new(
                    FACTORY_SCOPE_MISMATCH,
                    "role engine factory received the wrong review checkpoint authority",
                    false,
                ));
            }
        };
        Ok(RoleEngine::new(
            Arc::clone(&self.provider),
            Arc::new(runtime),
            Arc::clone(&self.events),
            Arc::clone(&self.redactor),
        ))
    }
}
