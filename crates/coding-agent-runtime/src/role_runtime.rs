use std::fmt;
use std::sync::Arc;

use coding_agent_core::{
    ActionRequest, AgentRuntime, ContextRedactor, ReviewDiffCheckpoint, ReviewDiffChunkRequest,
    ReviewDiffManifest, ReviewDiffRuntime, Role, RoleActionRuntime, RoleRun, RoleRuntimeResult,
    RuntimeActionRequest, RuntimeError, TerminalSnapshot, ToolRequest, ToolRuntime,
    ValidationRuntime, WorkspaceFingerprint,
};
use tokio_util::sync::CancellationToken;

use crate::RuntimeSession;

const ACTION_NOT_ALLOWED: &str = "ROLE_RUNTIME_ACTION_NOT_ALLOWED";
const CHECKPOINT_MISMATCH: &str = "ROLE_RUNTIME_CHECKPOINT_MISMATCH";

/// Capability wrapper around the one task-owned [`RuntimeSession`].
///
/// Core action validation is the first gate. This adapter is an independent
/// runtime-side gate and never accepts a control action in its type signature.
#[derive(Clone)]
pub struct RoleScopedRuntime {
    owner: RoleRun,
    session: Arc<RuntimeSession>,
    redactor: Arc<dyn ContextRedactor>,
    review_checkpoint: Option<ReviewDiffCheckpoint>,
}

impl RoleScopedRuntime {
    pub fn try_new(
        role: Role,
        role_run: u32,
        session: Arc<RuntimeSession>,
        redactor: Arc<dyn ContextRedactor>,
    ) -> Result<Self, RuntimeError> {
        let owner = RoleRun::try_new(role, role_run).map_err(|_| action_not_allowed())?;
        Ok(Self {
            owner,
            session,
            redactor,
            review_checkpoint: None,
        })
    }

    pub fn try_with_review_checkpoint(
        role: Role,
        role_run: u32,
        session: Arc<RuntimeSession>,
        redactor: Arc<dyn ContextRedactor>,
        checkpoint: ReviewDiffCheckpoint,
    ) -> Result<Self, RuntimeError> {
        if role != Role::Reviewer {
            return Err(action_not_allowed());
        }
        let mut scoped = Self::try_new(role, role_run, session, redactor)?;
        scoped.review_checkpoint = Some(checkpoint);
        Ok(scoped)
    }

    pub const fn owner(&self) -> RoleRun {
        self.owner
    }

    #[cfg(feature = "test-support")]
    pub fn shares_session_with(&self, session: &Arc<RuntimeSession>) -> bool {
        Arc::ptr_eq(&self.session, session)
    }

    fn permits(&self, request: &RuntimeActionRequest) -> bool {
        match (self.owner.role(), request) {
            (
                Role::Planner,
                RuntimeActionRequest::Tool(
                    ToolRequest::ListFiles { .. }
                    | ToolRequest::ReadFile { .. }
                    | ToolRequest::SearchText { .. },
                ),
            ) => true,
            (
                Role::Executor,
                RuntimeActionRequest::Tool(
                    ToolRequest::ListFiles { .. }
                    | ToolRequest::ReadFile { .. }
                    | ToolRequest::SearchText { .. }
                    | ToolRequest::ReplaceFile { .. }
                    | ToolRequest::GitStatus
                    | ToolRequest::GitDiff,
                ),
            ) => true,
            (
                Role::Reviewer,
                RuntimeActionRequest::Tool(
                    ToolRequest::ListFiles { .. }
                    | ToolRequest::ReadFile { .. }
                    | ToolRequest::SearchText { .. }
                    | ToolRequest::GitStatus
                    | ToolRequest::GitDiff,
                ),
            ) => true,
            (Role::Executor | Role::Reviewer, RuntimeActionRequest::Validation { .. }) => true,
            (
                Role::Reviewer,
                RuntimeActionRequest::ReviewDiffManifest { .. }
                | RuntimeActionRequest::ReviewDiffChunks { .. },
            ) => true,
            // Optional selectors must first be normalized and assigned a
            // core-owned CheckId; a runtime can execute only the exact check.
            (_, RuntimeActionRequest::ValidationSelector { .. }) => false,
            _ => false,
        }
    }
}

impl fmt::Debug for RoleScopedRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoleScopedRuntime")
            .field("owner", &self.owner)
            .field("has_review_checkpoint", &self.review_checkpoint.is_some())
            .field("session", &"<retained>")
            .finish()
    }
}

#[async_trait::async_trait]
impl RoleActionRuntime for RoleScopedRuntime {
    async fn invoke(
        &self,
        request: RuntimeActionRequest,
        cancellation: CancellationToken,
    ) -> Result<RoleRuntimeResult, RuntimeError> {
        if cancellation.is_cancelled() {
            return Err(RuntimeError::new(
                "COMMAND_CANCELLED",
                "runtime action was cancelled",
                false,
            ));
        }
        if ActionRequest::Runtime(request.clone()).validate().is_err() {
            return Err(action_not_allowed());
        }
        if !self.permits(&request) {
            return Err(action_not_allowed());
        }
        match request {
            RuntimeActionRequest::Tool(request) => self
                .session
                .invoke(request, cancellation)
                .await
                .map(RoleRuntimeResult::Tool),
            RuntimeActionRequest::Validation { check } => self
                .session
                .run_validation(check, cancellation)
                .await
                .map(RoleRuntimeResult::Validation),
            RuntimeActionRequest::ValidationSelector { .. } => Err(action_not_allowed()),
            RuntimeActionRequest::ReviewDiffManifest {
                generation,
                workspace_digest,
            } => {
                let checkpoint = self
                    .review_checkpoint
                    .as_ref()
                    .filter(|checkpoint| {
                        checkpoint.generation() == generation
                            && checkpoint.workspace_digest() == &workspace_digest
                    })
                    .cloned()
                    .ok_or_else(checkpoint_mismatch)?;
                self.session
                    .review_diff_manifest(checkpoint, Arc::clone(&self.redactor), cancellation)
                    .await
                    .map(RoleRuntimeResult::ReviewDiffManifest)
            }
            RuntimeActionRequest::ReviewDiffChunks {
                generation,
                workspace_digest,
                manifest_sha256,
                start_chunk,
                count,
            } => {
                let checkpoint = self
                    .review_checkpoint
                    .as_ref()
                    .filter(|checkpoint| {
                        checkpoint.generation() == generation
                            && checkpoint.workspace_digest() == &workspace_digest
                    })
                    .ok_or_else(checkpoint_mismatch)?;
                let request = ReviewDiffChunkRequest::try_exact(
                    checkpoint.generation(),
                    checkpoint.workspace_digest().clone(),
                    manifest_sha256,
                    start_chunk,
                    count,
                )
                .map_err(|_| checkpoint_mismatch())?;
                self.session
                    .review_diff_chunks(request, cancellation)
                    .await
                    .map(RoleRuntimeResult::ReviewDiffChunks)
            }
        }
    }

    async fn workspace_fingerprint(
        &self,
        cancellation: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError> {
        self.session.workspace_fingerprint(cancellation).await
    }

    async fn terminal_snapshot(
        &self,
        generation: u64,
        cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError> {
        self.session
            .terminal_snapshot(generation, cancellation)
            .await
    }

    async fn terminal_review_diff_manifest(
        &self,
        checkpoint: ReviewDiffCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<ReviewDiffManifest, RuntimeError> {
        let scoped = self
            .review_checkpoint
            .as_ref()
            .filter(|scoped| self.owner.role() == Role::Reviewer && **scoped == checkpoint)
            .cloned()
            .ok_or_else(checkpoint_mismatch)?;
        self.session
            .terminal_review_diff_manifest(scoped, Arc::clone(&self.redactor), cancellation)
            .await
    }
}

fn action_not_allowed() -> RuntimeError {
    RuntimeError::new(
        ACTION_NOT_ALLOWED,
        "the runtime action is not allowed for this role",
        false,
    )
}

fn checkpoint_mismatch() -> RuntimeError {
    RuntimeError::new(
        CHECKPOINT_MISMATCH,
        "the review diff action does not match the scoped checkpoint",
        false,
    )
}
