use coding_agent_domain::TaskId;
use sqlx::SqliteConnection;

use crate::StoreError;
use crate::delivery::{
    CleanupKind, CleanupOperationRecord, CleanupOperationState, DeliveryVersion,
    validate_cleanup_state,
};

use super::super::decode::{
    parse_branch_state, parse_cleanup_state, parse_version, parse_worktree_state,
};
use super::super::ownership_invariant;

struct HistoricalDisposition {
    version: DeliveryVersion,
    state: String,
    transition_id: i64,
    failure_code: Option<String>,
    transitioned_at: String,
}

struct CleanupTransitionRow {
    transition_id: i64,
    from_state: String,
    state: CleanupOperationState,
    failure_code: Option<String>,
    transitioned_at: String,
}

pub(in crate::delivery::ownership) async fn validate_cleanup_history(
    connection: &mut SqliteConnection,
    cleanup: &CleanupOperationRecord,
) -> Result<(), StoreError> {
    validate_target_head_observations(cleanup)?;
    let rows: Vec<(i64, String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT transition_id, from_state, to_state, failure_code, transitioned_at \
         FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'cleanup_operation' AND entity_id = ? \
         ORDER BY entity_version",
    )
    .bind(cleanup.operation_id.to_string())
    .fetch_all(&mut *connection)
    .await?;
    let mut previous_affected_version: Option<DeliveryVersion> = None;
    for (index, (transition_id, from_state, state, failure_code, transitioned_at)) in
        rows.into_iter().enumerate()
    {
        let transition = CleanupTransitionRow {
            transition_id,
            from_state,
            state: parse_cleanup_state(state)?,
            failure_code,
            transitioned_at,
        };
        let worktree = load_disposition_at(
            &mut *connection,
            "worktree_disposition",
            cleanup.disposition_task_id,
            transition.transition_id,
        )
        .await?;
        let branch = load_disposition_at(
            &mut *connection,
            "branch_disposition",
            cleanup.disposition_task_id,
            transition.transition_id,
        )
        .await?;
        validate_cleanup_state(
            cleanup.kind,
            transition.state,
            parse_worktree_state(worktree.state.clone())?,
            parse_branch_state(branch.state.clone())?,
        )
        .map_err(|_| ownership_invariant())?;
        validate_transition_target_head(cleanup, index, &transition)?;

        let affected = match cleanup.kind {
            CleanupKind::RemoveWorktree => &worktree,
            CleanupKind::DeleteBranch => &branch,
        };
        if affected.transition_id >= transition.transition_id {
            return Err(ownership_invariant());
        }
        if let Some(previous) = previous_affected_version {
            let fact_changed = cleanup_fact_changed(cleanup.kind, transition.state);
            let expected = if fact_changed {
                previous.next().map_err(|_| ownership_invariant())?
            } else {
                previous
            };
            if affected.version != expected {
                return Err(ownership_invariant());
            }
            if fact_changed
                && (affected.failure_code != transition.failure_code
                    || affected.transitioned_at != transition.transitioned_at)
            {
                return Err(ownership_invariant());
            }
        } else if index != 0 {
            return Err(ownership_invariant());
        }
        previous_affected_version = Some(affected.version);
    }
    Ok(())
}

fn validate_target_head_observations(cleanup: &CleanupOperationRecord) -> Result<(), StoreError> {
    match cleanup.kind {
        CleanupKind::RemoveWorktree => {
            if cleanup.target_head_observations.is_empty() {
                return Ok(());
            }
        }
        CleanupKind::DeleteBranch => {
            let count = u64::try_from(cleanup.target_head_observations.len())
                .map_err(|_| ownership_invariant())?;
            let endpoints_are_exact =
                cleanup
                    .target_head_observations
                    .first()
                    .is_some_and(|observation| {
                        Some(&observation.target_head) == cleanup.origin_target_head.as_ref()
                    })
                    && cleanup
                        .target_head_observations
                        .last()
                        .is_some_and(|observation| {
                            Some(&observation.target_head) == cleanup.expected_target_head.as_ref()
                        });
            if count != cleanup.version.get() || !endpoints_are_exact {
                return Err(ownership_invariant());
            }
            for (index, observation) in cleanup.target_head_observations.iter().enumerate() {
                let expected_version = u64::try_from(index)
                    .ok()
                    .and_then(|value| value.checked_add(1))
                    .and_then(|value| DeliveryVersion::try_new(value).ok())
                    .ok_or_else(ownership_invariant)?;
                if observation.operation_version != expected_version {
                    return Err(ownership_invariant());
                }
                if observation.target_head.algorithm() != cleanup.expected_source_oid.algorithm() {
                    return Err(ownership_invariant());
                }
            }
            return Ok(());
        }
    }
    Err(ownership_invariant())
}

fn validate_transition_target_head(
    cleanup: &CleanupOperationRecord,
    index: usize,
    transition: &CleanupTransitionRow,
) -> Result<(), StoreError> {
    if cleanup.kind == CleanupKind::RemoveWorktree {
        return Ok(());
    }
    let observation = cleanup
        .target_head_observations
        .get(index)
        .ok_or_else(ownership_invariant)?;
    if observation.observed_at.to_string() != transition.transitioned_at {
        return Err(ownership_invariant());
    }
    if let Some(previous) = index
        .checked_sub(1)
        .and_then(|previous| cleanup.target_head_observations.get(previous))
    {
        let refreshed = transition.from_state == CleanupOperationState::DeletePending.as_str()
            && transition.state == CleanupOperationState::DeletePending;
        if refreshed == (previous.target_head == observation.target_head) {
            return Err(ownership_invariant());
        }
    }
    Ok(())
}

fn cleanup_fact_changed(kind: CleanupKind, state: CleanupOperationState) -> bool {
    matches!(
        (kind, state),
        (
            CleanupKind::RemoveWorktree,
            CleanupOperationState::UnlockedPendingRemove
                | CleanupOperationState::Completed
                | CleanupOperationState::ReconciliationRequired
        ) | (
            CleanupKind::DeleteBranch,
            CleanupOperationState::Completed | CleanupOperationState::ReconciliationRequired
        )
    )
}

async fn load_disposition_at(
    connection: &mut SqliteConnection,
    entity_kind: &str,
    task_id: TaskId,
    transition_id: i64,
) -> Result<HistoricalDisposition, StoreError> {
    let row: Option<(i64, String, i64, Option<String>, String)> = sqlx::query_as(
        "SELECT entity_version, to_state, transition_id, failure_code, transitioned_at \
         FROM task_delivery_operation_transitions \
         WHERE entity_kind = ? AND entity_id = ? AND transition_id <= ? \
         ORDER BY transition_id DESC LIMIT 1",
    )
    .bind(entity_kind)
    .bind(task_id.to_string())
    .bind(transition_id)
    .fetch_optional(connection)
    .await?;
    let (version, state, transition_id, failure_code, transitioned_at) =
        row.ok_or_else(ownership_invariant)?;
    Ok(HistoricalDisposition {
        version: parse_version(version)?,
        state,
        transition_id,
        failure_code,
        transitioned_at,
    })
}
