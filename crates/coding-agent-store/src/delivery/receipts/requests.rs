use coding_agent_domain::{ClientRequestId, TaskId};

use crate::delivery::{DeliveryCommandId, DeliveryError};

mod accept;
mod cleanup;
mod preflight;

pub use accept::AcceptMergeCommandRequest;
pub use cleanup::{DeleteBranchCommandRequest, RemoveWorktreeCommandRequest};
pub use preflight::PreflightCommandRequest;

fn validate_request_ids(
    client_request_id: ClientRequestId,
    task_id: TaskId,
) -> Result<DeliveryCommandId, DeliveryError> {
    if client_request_id.as_uuid().is_nil() || task_id.as_uuid().is_nil() {
        return Err(DeliveryError::InvalidCommandRequest);
    }
    DeliveryCommandId::try_from(client_request_id).map_err(|_| DeliveryError::InvalidCommandRequest)
}

fn domain_client_request_id(value: DeliveryCommandId) -> ClientRequestId {
    value
        .to_string()
        .parse()
        .expect("validated delivery command IDs convert to domain request IDs")
}

fn parse_task_id(value: &str) -> Result<TaskId, DeliveryError> {
    let parsed = value
        .parse::<TaskId>()
        .map_err(|_| DeliveryError::InvalidCommandRequest)?;
    if parsed.as_uuid().is_nil() || parsed.to_string() != value {
        Err(DeliveryError::InvalidCommandRequest)
    } else {
        Ok(parsed)
    }
}
