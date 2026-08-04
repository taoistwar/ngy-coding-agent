use coding_agent_domain::{ClientRequestId, Task};
use coding_agent_store::{
    CreatePreflightOutcome, CreatePreflightRequest, DeliveryCommandReceipt, DirectoryIdentity,
    GitBranchRef, GitCommitOid, GitTreeOid, PreflightCommandRequest, Sha256Digest, Store,
};

use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, CANDIDATE_TREE, COMMON_IDENTITY, CONFIG_DIGEST, PREFLIGHT_SOURCE, TARGET_HEAD,
    approved_task_with_ready_artifact,
};

pub async fn eligible_fixture() -> (Store, Task) {
    approved_task_with_ready_artifact("codex/task-receipt").await
}

pub fn preflight_request(
    task: &Task,
    client_request_id: ClientRequestId,
) -> CreatePreflightRequest {
    let command = PreflightCommandRequest::try_new(
        client_request_id,
        task.id,
        "refs/heads/main".parse::<GitBranchRef>().unwrap(),
        TARGET_HEAD.parse::<GitCommitOid>().unwrap(),
    )
    .unwrap();
    CreatePreflightRequest::try_new(
        command,
        CANDIDATE_TREE.parse::<GitTreeOid>().unwrap(),
        PREFLIGHT_SOURCE.parse::<GitCommitOid>().unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        CONFIG_DIGEST.parse::<Sha256Digest>().unwrap(),
    )
    .unwrap()
}

pub fn receipt(outcome: CreatePreflightOutcome) -> DeliveryCommandReceipt {
    match outcome {
        CreatePreflightOutcome::Created(receipt) | CreatePreflightOutcome::Existing(receipt) => {
            receipt
        }
    }
}

pub async fn row_counts(store: &Store) -> (i64, i64, i64) {
    let merge_rows = sqlx::query_scalar("SELECT COUNT(*) FROM task_merge_operations")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let transitions = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    let receipts = sqlx::query_scalar("SELECT COUNT(*) FROM task_delivery_command_receipts")
        .fetch_one(store.pool())
        .await
        .unwrap();
    (merge_rows, transitions, receipts)
}
