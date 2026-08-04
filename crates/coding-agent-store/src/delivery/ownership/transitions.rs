use sqlx::SqliteConnection;

use crate::StoreError;

use super::ownership_invariant;

mod bounds;
mod cleanup;
mod disposition;
mod merge;
mod source;

pub(super) use bounds::transition_bounds;
pub(super) use disposition::disposition_state_at;
pub(super) use source::source_state_at;

async fn transition_pair_is_invalid(
    connection: &mut SqliteConnection,
    entity_kind: &str,
    entity_id: &str,
) -> Result<bool, StoreError> {
    match entity_kind {
        "delivery_source" => source::transition_pair_is_invalid(connection, entity_id).await,
        "merge_operation" => merge::transition_pair_is_invalid(connection, entity_id).await,
        "cleanup_operation" => cleanup::transition_pair_is_invalid(connection, entity_id).await,
        "worktree_disposition" | "branch_disposition" => {
            disposition::transition_pair_is_invalid(connection, entity_kind, entity_id).await
        }
        _ => Err(ownership_invariant()),
    }
}
