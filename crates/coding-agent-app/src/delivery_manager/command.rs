#[cfg(feature = "test-support")]
use coding_agent_domain::RepositoryId;
use coding_agent_domain::TaskId;
use coding_agent_store::DeliveryOperationId;
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};

use crate::{
    DeliveryCleanupAcceptanceOutcome, DeliveryManagerQuiesceSnapshot,
    DeliveryMergeAcceptanceOutcome, DeliveryOperationQueryOutcome, DeliveryPreflightOutcome,
    DeliveryTaskQueryOutcome, RepositoryControlLease, ServiceStateSnapshot,
};

use super::shutdown::DeliveryManagerShutdownProof;
use super::{
    DeliveryAcceptRequest, DeliveryDeleteBranchRequest, DeliveryOperationRecoveryOutcome,
    DeliveryPreflightRequest, DeliveryRemoveWorktreeRequest,
};

pub(super) enum DeliveryManagerCommand {
    Query {
        task_id: TaskId,
        completion_sender: mpsc::Sender<DeliveryManagerCommand>,
        response: oneshot::Sender<DeliveryTaskQueryOutcome>,
    },
    OperationQuery {
        operation_id: DeliveryOperationId,
        completion_sender: mpsc::Sender<DeliveryManagerCommand>,
        response: oneshot::Sender<DeliveryOperationQueryOutcome>,
    },
    Preflight {
        request: DeliveryPreflightRequest,
        completion_sender: mpsc::Sender<DeliveryManagerCommand>,
        response: oneshot::Sender<DeliveryPreflightOutcome>,
    },
    AcceptMerge {
        request: DeliveryAcceptRequest,
        completion_sender: mpsc::Sender<DeliveryManagerCommand>,
        response: oneshot::Sender<DeliveryMergeAcceptanceOutcome>,
    },
    RemoveWorktree {
        request: DeliveryRemoveWorktreeRequest,
        completion_sender: mpsc::Sender<DeliveryManagerCommand>,
        response: oneshot::Sender<DeliveryCleanupAcceptanceOutcome>,
    },
    DeleteBranch {
        request: DeliveryDeleteBranchRequest,
        completion_sender: mpsc::Sender<DeliveryManagerCommand>,
        response: oneshot::Sender<DeliveryCleanupAcceptanceOutcome>,
    },
    RecoverOperation {
        operation_id: DeliveryOperationId,
        completion_sender: mpsc::Sender<DeliveryManagerCommand>,
        response: oneshot::Sender<DeliveryOperationRecoveryOutcome>,
    },
    WorkerCompleted {
        worker_id: u64,
        completion: Box<DeliveryWorkerCompletion>,
    },
    ServiceChanged(ServiceStateSnapshot),
    Quiesce {
        response: oneshot::Sender<DeliveryManagerQuiesceSnapshot>,
    },
    ShutdownAndJoin {
        response: oneshot::Sender<DeliveryManagerShutdownProof>,
    },
    #[cfg(feature = "test-support")]
    RetainFailClosedForTest {
        repository_id: RepositoryId,
        response: oneshot::Sender<bool>,
    },
}

pub(super) enum DeliveryWorkerCompletion {
    Query {
        outcome: Box<DeliveryTaskQueryOutcome>,
        response: oneshot::Sender<DeliveryTaskQueryOutcome>,
    },
    OperationQuery {
        outcome: DeliveryOperationQueryOutcome,
        response: oneshot::Sender<DeliveryOperationQueryOutcome>,
    },
    Preflight {
        outcome: DeliveryPreflightOutcome,
        retention: DeliveryWorkerRetention,
        response: oneshot::Sender<DeliveryPreflightOutcome>,
    },
    Merge {
        retention: DeliveryWorkerRetention,
    },
    Cleanup {
        retention: DeliveryWorkerRetention,
    },
    Recovery {
        outcome: DeliveryOperationRecoveryOutcome,
        retention: DeliveryWorkerRetention,
        response: oneshot::Sender<DeliveryOperationRecoveryOutcome>,
    },
}

pub(super) enum DeliveryWorkerRetention {
    Released,
    RetainedFailClosed(DeliveryWorkerRetainedOwnership),
}

pub(super) struct DeliveryWorkerRetainedOwnership {
    _global_permit: OwnedSemaphorePermit,
    _repository_lease: RepositoryControlLease,
}

impl DeliveryWorkerRetainedOwnership {
    pub(super) fn new(
        global_permit: OwnedSemaphorePermit,
        repository_lease: RepositoryControlLease,
    ) -> Self {
        Self {
            _global_permit: global_permit,
            _repository_lease: repository_lease,
        }
    }
}
