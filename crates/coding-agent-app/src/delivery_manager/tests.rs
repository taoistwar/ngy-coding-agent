use std::str::FromStr;
use std::sync::Arc;

use coding_agent_domain::{ClientRequestId, TaskId};
use coding_agent_store::{GitBranchRef, GitCommitOid, PreflightCommandRequest};

use crate::{
    DeliveryPreflightOutcome, DeliveryPreflightUnavailableReason, DeliveryQueryUnavailableReason,
    DeliveryTaskQueryOutcome, RepositoryControlCoordinator, ServiceState, ServiceStateController,
};

use super::*;

fn request() -> DeliveryPreflightRequest {
    DeliveryPreflightRequest::new(
        PreflightCommandRequest::try_new(
            ClientRequestId::new(),
            TaskId::new(),
            GitBranchRef::from_str("refs/heads/main").expect("valid target branch"),
            GitCommitOid::from_str("0123456789abcdef0123456789abcdef01234567")
                .expect("valid target head"),
        )
        .expect("valid preflight command"),
    )
}

#[tokio::test]
async fn unavailable_composition_is_bounded_and_never_manufactures_acceptance() {
    let manager = DeliveryManagerHandle::spawn_unavailable(
        Arc::new(RepositoryControlCoordinator::new()),
        ServiceStateController::new(ServiceState::Ready),
        2,
    );
    assert_eq!(
        manager
            .preflight(request())
            .await
            .expect("actor remains open"),
        DeliveryPreflightOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::OrchestrationUnavailable
        )
    );
    let outcome = manager
        .query(TaskId::new())
        .await
        .expect("actor remains open");
    assert!(matches!(
        outcome,
        DeliveryTaskQueryOutcome::Unavailable {
            reason: DeliveryQueryUnavailableReason::OrchestrationUnavailable,
            ..
        }
    ));
}

#[tokio::test]
async fn quiesce_closes_new_unavailable_intake() {
    let manager = DeliveryManagerHandle::spawn_unavailable(
        Arc::new(RepositoryControlCoordinator::new()),
        ServiceStateController::new(ServiceState::Ready),
        2,
    );
    let snapshot = manager.quiesce().await.expect("quiesce actor");
    assert_eq!(snapshot.in_flight_workers(), 0);
    assert_eq!(snapshot.queued_workers(), 0);
    assert_eq!(
        manager
            .preflight(request())
            .await
            .expect("actor remains open"),
        DeliveryPreflightOutcome::Unavailable(DeliveryPreflightUnavailableReason::ManagerQuiescing)
    );
}

#[test]
fn request_debug_is_redacted() {
    let request = request();
    let debug = format!("{request:?}");
    assert!(debug.contains(&request.task_id().to_string()));
    assert!(!debug.contains("refs/heads/main"));
    assert!(!debug.contains("0123456789abcdef"));
}
