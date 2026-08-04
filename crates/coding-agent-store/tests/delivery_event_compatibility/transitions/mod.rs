mod branch;
mod helpers;
mod merge;
mod preflight;
mod scenario;
mod source;
mod worktree;

use coding_agent_domain::Task;
use coding_agent_store::Store;

use crate::snapshot::CompatibilitySnapshot;

pub use preflight::{create_preflight, record_conflict};

pub(super) const TARGET_BRANCH: &str = "refs/heads/main";

pub async fn exercise_every_delivery_transition(
    store: &Store,
    task: &Task,
    baseline: &CompatibilitySnapshot,
) {
    preflight::close_conflicting_preflight(store, task, baseline).await;
    let accepted = preflight::accept_ready_preflight(store, task, baseline).await;
    source::commit_delivery_source(store, task, &accepted, baseline).await;
    merge::complete_delivery_merge(store, task, &accepted, baseline).await;
    worktree::remove_worktree(store, task, accepted.operation_id, baseline).await;
    branch::delete_branch(store, task, accepted.operation_id, baseline).await;
}

pub async fn exercise_every_alternate_delivery_transition() {
    preflight::exercise_terminal_preflight_transitions().await;
    source::exercise_retry_and_reconcile_transitions().await;
    merge::exercise_failure_abort_and_reconcile_transitions().await;
    worktree::exercise_failure_retry_and_reconcile_transitions().await;
    branch::exercise_failure_retry_and_reconcile_transitions().await;
}
