use super::*;

pub(super) async fn bind_fresh_cleanup_runtime(
    dependencies: &DeliveryManagerLiveDependencies,
    snapshot: &DeliveryEligibilitySnapshot,
    command: &CleanupCommand,
) -> Result<(), DeliveryLiveCleanupRuntimeError> {
    let registry = dependencies
        .cleanup_runtime_registry
        .as_ref()
        .ok_or(DeliveryLiveCleanupRuntimeError::Unavailable)?;
    let session = timeout(
        LIVE_RUNTIME_STAGE_TIMEOUT,
        registry.open_cleanup_session(snapshot),
    )
    .await
    .map_err(|_| DeliveryLiveCleanupRuntimeError::Unavailable)??;
    match command {
        CleanupCommand::RemoveWorktree(command) => timeout(
            LIVE_RUNTIME_STAGE_TIMEOUT,
            session.bind_worktree_cleanup(
                snapshot,
                DeliveryWorktreeCleanupBinding::Acceptance(command),
            ),
        )
        .await
        .map_err(|_| DeliveryLiveCleanupRuntimeError::Unavailable)?
        .map(drop),
        CleanupCommand::DeleteBranch(command) => timeout(
            LIVE_RUNTIME_STAGE_TIMEOUT,
            session
                .bind_branch_cleanup(snapshot, DeliveryBranchCleanupBinding::Acceptance(command)),
        )
        .await
        .map_err(|_| DeliveryLiveCleanupRuntimeError::Unavailable)?
        .map(drop),
    }
}
