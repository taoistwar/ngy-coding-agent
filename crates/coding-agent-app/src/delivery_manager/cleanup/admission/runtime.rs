use super::*;
use crate::delivery_manager::runtime_stage::{ProcessStageCompletion, run_process_stage};

pub(super) enum FreshCleanupRuntimeBindingError {
    Runtime(DeliveryLiveCleanupRuntimeError),
    TimedOutWithCleanupUnproven,
}

pub(super) async fn bind_fresh_cleanup_runtime(
    dependencies: &DeliveryManagerLiveDependencies,
    snapshot: &DeliveryEligibilitySnapshot,
    command: &CleanupCommand,
) -> Result<(), FreshCleanupRuntimeBindingError> {
    let registry = dependencies.cleanup_runtime_registry.as_ref().ok_or(
        FreshCleanupRuntimeBindingError::Runtime(DeliveryLiveCleanupRuntimeError::Unavailable),
    )?;
    let session = timeout(
        LIVE_RUNTIME_STAGE_TIMEOUT,
        registry.open_cleanup_session(snapshot),
    )
    .await
    .map_err(|_| {
        FreshCleanupRuntimeBindingError::Runtime(DeliveryLiveCleanupRuntimeError::Unavailable)
    })?
    .map_err(FreshCleanupRuntimeBindingError::Runtime)?;
    match command {
        CleanupCommand::RemoveWorktree(command) => match run_process_stage(
            LIVE_RUNTIME_STAGE_TIMEOUT,
            session.bind_worktree_cleanup(
                snapshot,
                DeliveryWorktreeCleanupBinding::Acceptance(command),
            ),
        )
        .await
        {
            ProcessStageCompletion::Completed(result) => result
                .map(drop)
                .map_err(FreshCleanupRuntimeBindingError::Runtime),
            ProcessStageCompletion::TimedOutWithCleanupUnproven => {
                Err(FreshCleanupRuntimeBindingError::TimedOutWithCleanupUnproven)
            }
        },
        CleanupCommand::DeleteBranch(command) => match run_process_stage(
            LIVE_RUNTIME_STAGE_TIMEOUT,
            session
                .bind_branch_cleanup(snapshot, DeliveryBranchCleanupBinding::Acceptance(command)),
        )
        .await
        {
            ProcessStageCompletion::Completed(result) => result
                .map(drop)
                .map_err(FreshCleanupRuntimeBindingError::Runtime),
            ProcessStageCompletion::TimedOutWithCleanupUnproven => {
                Err(FreshCleanupRuntimeBindingError::TimedOutWithCleanupUnproven)
            }
        },
    }
}
