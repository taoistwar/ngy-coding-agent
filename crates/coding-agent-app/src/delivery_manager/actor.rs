use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::{
    DeliveryCleanupAcceptanceOutcome, DeliveryMergeAcceptanceOutcome,
    DeliveryOperationQueryOutcome, DeliveryPreflightBusyReason, DeliveryPreflightOutcome,
    DeliveryQueryUnavailableReason, DeliveryTaskQueryOutcome, RepositoryControlCoordinator,
    ServiceState, ServiceStateController, ServiceStateSnapshot,
};

use super::command::{
    DeliveryManagerCommand, DeliveryWorkerCompletion, DeliveryWorkerRetainedOwnership,
    DeliveryWorkerRetention,
};
use super::shutdown::{self, DeliveryManagerShutdownProof};
use super::{
    DeliveryManagerBackend, DeliveryManagerQuiesceSnapshot, DeliveryOperationRecoveryOutcome,
    cleanup, merge, operation_query, preflight, query, recovery,
};

const DELIVERY_QUERY_WORKER_LIMIT: usize = 2;

pub(crate) struct DeliveryIntakeGate {
    quiesced: AtomicBool,
    generation: AtomicU64,
}

impl DeliveryIntakeGate {
    pub(super) fn new(quiesced: bool) -> Self {
        Self {
            quiesced: AtomicBool::new(quiesced),
            generation: AtomicU64::new(0),
        }
    }

    pub(super) fn close(&self) {
        if !self.quiesced.swap(true, Ordering::AcqRel) {
            self.generation.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub(crate) fn snapshot(&self) -> (bool, u64) {
        (
            self.quiesced.load(Ordering::Acquire),
            self.generation.load(Ordering::Acquire),
        )
    }

    pub(crate) fn still_accepts(&self, generation: u64) -> bool {
        !self.quiesced.load(Ordering::Acquire)
            && self.generation.load(Ordering::Acquire) == generation
    }
}

pub(super) struct DeliveryManager {
    pub(super) receiver: mpsc::Receiver<DeliveryManagerCommand>,
    pub(super) backend: DeliveryManagerBackend,
    pub(super) repository_control: Arc<RepositoryControlCoordinator>,
    pub(super) global_git_operations: Arc<Semaphore>,
    pub(super) service: ServiceStateSnapshot,
    pub(super) service_state: ServiceStateController,
    pub(super) intake_gate: Arc<DeliveryIntakeGate>,
    pub(super) query_workers: HashSet<u64>,
    pub(super) mutation_workers: HashSet<u64>,
    pub(super) retained_fail_closed: HashMap<u64, DeliveryWorkerRetainedOwnership>,
    pub(super) pending_queries: VecDeque<DeliveryManagerCommand>,
    pub(super) pending_mutations: VecDeque<DeliveryManagerCommand>,
    pub(super) worker_limit: usize,
    pub(super) pending_limit: usize,
    pub(super) next_worker_id: u64,
    pub(super) hard_shutdown: bool,
    pub(super) shutdown_waiters: Vec<oneshot::Sender<DeliveryManagerShutdownProof>>,
}

impl DeliveryManager {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        receiver: mpsc::Receiver<DeliveryManagerCommand>,
        backend: DeliveryManagerBackend,
        repository_control: Arc<RepositoryControlCoordinator>,
        global_git_operations: Arc<Semaphore>,
        service: ServiceStateSnapshot,
        service_state: ServiceStateController,
        intake_gate: Arc<DeliveryIntakeGate>,
        capacity: usize,
    ) -> Self {
        Self {
            receiver,
            backend,
            repository_control,
            global_git_operations,
            service,
            service_state,
            intake_gate,
            query_workers: HashSet::new(),
            mutation_workers: HashSet::new(),
            retained_fail_closed: HashMap::new(),
            pending_queries: VecDeque::with_capacity(capacity),
            pending_mutations: VecDeque::with_capacity(capacity),
            worker_limit: capacity,
            pending_limit: capacity,
            next_worker_id: 1,
            hard_shutdown: false,
            shutdown_waiters: Vec::new(),
        }
    }

    pub(super) async fn run(mut self) {
        while let Some(command) = self.receiver.recv().await {
            self.handle(command);
        }
    }

    fn handle(&mut self, command: DeliveryManagerCommand) {
        match command {
            command @ (DeliveryManagerCommand::Query { .. }
            | DeliveryManagerCommand::OperationQuery { .. }
            | DeliveryManagerCommand::Preflight { .. }
            | DeliveryManagerCommand::AcceptMerge { .. }
            | DeliveryManagerCommand::RemoveWorktree { .. }
            | DeliveryManagerCommand::DeleteBranch { .. }
            | DeliveryManagerCommand::RecoverOperation { .. }) => {
                if self.hard_shutdown {
                    shutdown::reject_after_shutdown(command);
                } else {
                    self.admit_worker(command);
                }
            }
            DeliveryManagerCommand::WorkerCompleted {
                worker_id,
                completion,
            } => {
                match *completion {
                    DeliveryWorkerCompletion::Query { outcome, response } => {
                        let known_worker = self.query_workers.remove(&worker_id);
                        debug_assert!(known_worker, "delivery query completion must be exact");
                        let _ = response.send(*outcome);
                    }
                    DeliveryWorkerCompletion::OperationQuery { outcome, response } => {
                        let known_worker = self.query_workers.remove(&worker_id);
                        debug_assert!(
                            known_worker,
                            "delivery operation query completion must be exact"
                        );
                        let _ = response.send(outcome);
                    }
                    DeliveryWorkerCompletion::Preflight {
                        outcome,
                        retention,
                        response,
                    } => {
                        let known_worker = self.mutation_workers.contains(&worker_id);
                        debug_assert!(known_worker, "delivery preflight completion must be exact");
                        match retention {
                            DeliveryWorkerRetention::Released => {
                                self.mutation_workers.remove(&worker_id);
                            }
                            DeliveryWorkerRetention::RetainedFailClosed(ownership) => {
                                let previous =
                                    self.retained_fail_closed.insert(worker_id, ownership);
                                debug_assert!(
                                    previous.is_none(),
                                    "fail-closed retention is single-shot"
                                );
                            }
                        }
                        let _ = response.send(outcome);
                    }
                    DeliveryWorkerCompletion::Merge { retention } => {
                        self.finish_mutation_worker(worker_id, retention);
                    }
                    DeliveryWorkerCompletion::Cleanup { retention } => {
                        self.finish_mutation_worker(worker_id, retention);
                    }
                    DeliveryWorkerCompletion::Recovery {
                        outcome,
                        retention,
                        response,
                    } => {
                        self.finish_mutation_worker(worker_id, retention);
                        let _ = response.send(outcome);
                    }
                }
                self.start_queued_workers();
            }
            DeliveryManagerCommand::ServiceChanged(snapshot) => {
                self.service = snapshot;
                if snapshot.state == ServiceState::Quiescing {
                    self.intake_gate.close();
                }
            }
            DeliveryManagerCommand::Quiesce { response } => {
                self.intake_gate.close();
                let _ = response.send(DeliveryManagerQuiesceSnapshot {
                    in_flight_workers: self.query_workers.len() + self.mutation_workers.len(),
                    queued_workers: self.pending_queries.len() + self.pending_mutations.len(),
                });
            }
            DeliveryManagerCommand::ShutdownAndJoin { response } => {
                self.begin_shutdown_join(response);
            }
            #[cfg(feature = "test-support")]
            DeliveryManagerCommand::RetainFailClosedForTest {
                repository_id,
                response,
            } => {
                let retained =
                    !self.hard_shutdown && self.retain_fail_closed_for_test(repository_id);
                let _ = response.send(retained);
            }
        }
        self.complete_shutdown_join_if_ready();
    }

    fn admit_worker(&mut self, command: DeliveryManagerCommand) {
        match command {
            command @ (DeliveryManagerCommand::Query { .. }
            | DeliveryManagerCommand::OperationQuery { .. }) => {
                if self.query_workers.len() < DELIVERY_QUERY_WORKER_LIMIT {
                    self.start_worker(command);
                } else if self.pending_queries.len() < self.pending_limit {
                    self.pending_queries.push_back(command);
                } else {
                    reject_worker_queue_full(command);
                }
            }
            command @ (DeliveryManagerCommand::Preflight { .. }
            | DeliveryManagerCommand::AcceptMerge { .. }
            | DeliveryManagerCommand::RemoveWorktree { .. }
            | DeliveryManagerCommand::DeleteBranch { .. }
            | DeliveryManagerCommand::RecoverOperation { .. }) => {
                if self.mutation_workers.len() < self.worker_limit {
                    self.start_worker(command);
                } else if self.pending_mutations.len() < self.pending_limit {
                    self.pending_mutations.push_back(command);
                } else {
                    reject_worker_queue_full(command);
                }
            }
            _ => unreachable!("only worker commands are admitted"),
        }
    }

    pub(super) fn start_queued_workers(&mut self) {
        while self.query_workers.len() < DELIVERY_QUERY_WORKER_LIMIT {
            let Some(command) = self.pending_queries.pop_front() else {
                break;
            };
            self.start_worker(command);
        }
        while self.mutation_workers.len() < self.worker_limit {
            let Some(command) = self.pending_mutations.pop_front() else {
                break;
            };
            self.start_worker(command);
        }
    }

    fn start_worker(&mut self, command: DeliveryManagerCommand) {
        let Some(worker_id) = self.allocate_worker_id() else {
            reject_worker_queue_full(command);
            return;
        };
        match command {
            DeliveryManagerCommand::Query {
                task_id,
                completion_sender,
                response,
            } => {
                self.query_workers.insert(worker_id);
                query::spawn_query_worker(
                    worker_id,
                    self.backend.clone(),
                    self.service,
                    task_id,
                    completion_sender,
                    response,
                )
            }
            DeliveryManagerCommand::OperationQuery {
                operation_id,
                completion_sender,
                response,
            } => {
                self.query_workers.insert(worker_id);
                operation_query::spawn_operation_query_worker(
                    worker_id,
                    self.backend.clone(),
                    operation_id,
                    completion_sender,
                    response,
                )
            }
            DeliveryManagerCommand::Preflight {
                request,
                completion_sender,
                response,
            } => {
                self.mutation_workers.insert(worker_id);
                preflight::spawn_preflight_worker(
                    worker_id,
                    Arc::clone(&self.global_git_operations),
                    Arc::clone(&self.repository_control),
                    Arc::clone(&self.intake_gate),
                    self.service_state.clone(),
                    self.backend.clone(),
                    self.service,
                    request,
                    completion_sender,
                    response,
                )
            }
            DeliveryManagerCommand::AcceptMerge {
                request,
                completion_sender,
                response,
            } => {
                self.mutation_workers.insert(worker_id);
                merge::spawn_accept_worker(
                    worker_id,
                    Arc::clone(&self.global_git_operations),
                    Arc::clone(&self.repository_control),
                    Arc::clone(&self.intake_gate),
                    self.service_state.clone(),
                    self.backend.clone(),
                    self.service,
                    request,
                    completion_sender,
                    response,
                )
            }
            DeliveryManagerCommand::RemoveWorktree {
                request,
                completion_sender,
                response,
            } => {
                self.mutation_workers.insert(worker_id);
                cleanup::spawn_remove_worktree_worker(
                    worker_id,
                    Arc::clone(&self.global_git_operations),
                    Arc::clone(&self.repository_control),
                    Arc::clone(&self.intake_gate),
                    self.service_state.clone(),
                    self.backend.clone(),
                    self.service,
                    request,
                    completion_sender,
                    response,
                )
            }
            DeliveryManagerCommand::DeleteBranch {
                request,
                completion_sender,
                response,
            } => {
                self.mutation_workers.insert(worker_id);
                cleanup::spawn_delete_branch_worker(
                    worker_id,
                    Arc::clone(&self.global_git_operations),
                    Arc::clone(&self.repository_control),
                    Arc::clone(&self.intake_gate),
                    self.service_state.clone(),
                    self.backend.clone(),
                    self.service,
                    request,
                    completion_sender,
                    response,
                )
            }
            DeliveryManagerCommand::RecoverOperation {
                operation_id,
                completion_sender,
                response,
            } => {
                self.mutation_workers.insert(worker_id);
                recovery::spawn_operation_recovery_worker(
                    worker_id,
                    Arc::clone(&self.global_git_operations),
                    Arc::clone(&self.repository_control),
                    self.backend.clone(),
                    operation_id,
                    completion_sender,
                    response,
                )
            }
            DeliveryManagerCommand::WorkerCompleted { .. }
            | DeliveryManagerCommand::ServiceChanged(_)
            | DeliveryManagerCommand::Quiesce { .. }
            | DeliveryManagerCommand::ShutdownAndJoin { .. } => {
                unreachable!("only worker commands are admitted")
            }
            #[cfg(feature = "test-support")]
            DeliveryManagerCommand::RetainFailClosedForTest { .. } => {
                unreachable!("test control commands are handled by the actor")
            }
        }
    }

    pub(super) fn allocate_worker_id(&mut self) -> Option<u64> {
        let worker_id = self.next_worker_id;
        self.next_worker_id = self.next_worker_id.checked_add(1)?;
        (!self.query_workers.contains(&worker_id) && !self.mutation_workers.contains(&worker_id))
            .then_some(worker_id)
    }

    fn finish_mutation_worker(&mut self, worker_id: u64, retention: DeliveryWorkerRetention) {
        let known_worker = self.mutation_workers.contains(&worker_id);
        debug_assert!(known_worker, "delivery mutation completion must be exact");
        match retention {
            DeliveryWorkerRetention::Released => {
                self.mutation_workers.remove(&worker_id);
            }
            DeliveryWorkerRetention::RetainedFailClosed(ownership) => {
                let previous = self.retained_fail_closed.insert(worker_id, ownership);
                debug_assert!(previous.is_none(), "fail-closed retention is single-shot");
            }
        }
    }
}

fn reject_worker_queue_full(command: DeliveryManagerCommand) {
    match command {
        DeliveryManagerCommand::Query {
            task_id, response, ..
        } => {
            let _ = response.send(DeliveryTaskQueryOutcome::unavailable(
                task_id,
                DeliveryQueryUnavailableReason::OrchestrationUnavailable,
            ));
        }
        DeliveryManagerCommand::OperationQuery {
            operation_id,
            response,
            ..
        } => {
            let _ = response.send(DeliveryOperationQueryOutcome::unavailable(
                operation_id,
                DeliveryQueryUnavailableReason::OrchestrationUnavailable,
            ));
        }
        DeliveryManagerCommand::Preflight { response, .. } => {
            let _ = response.send(DeliveryPreflightOutcome::Busy(
                DeliveryPreflightBusyReason::WorkerQueueFull,
            ));
        }
        DeliveryManagerCommand::AcceptMerge { response, .. } => {
            let _ = response.send(DeliveryMergeAcceptanceOutcome::Busy(
                DeliveryPreflightBusyReason::WorkerQueueFull,
            ));
        }
        DeliveryManagerCommand::RemoveWorktree { response, .. }
        | DeliveryManagerCommand::DeleteBranch { response, .. } => {
            let _ = response.send(DeliveryCleanupAcceptanceOutcome::Busy(
                DeliveryPreflightBusyReason::WorkerQueueFull,
            ));
        }
        DeliveryManagerCommand::RecoverOperation { response, .. } => {
            let _ = response.send(DeliveryOperationRecoveryOutcome::Unavailable);
        }
        DeliveryManagerCommand::WorkerCompleted { .. }
        | DeliveryManagerCommand::ServiceChanged(_)
        | DeliveryManagerCommand::Quiesce { .. }
        | DeliveryManagerCommand::ShutdownAndJoin { .. } => {
            unreachable!("only worker commands can overflow the worker queue")
        }
        #[cfg(feature = "test-support")]
        DeliveryManagerCommand::RetainFailClosedForTest { .. } => {
            unreachable!("test control commands do not enter the worker queue")
        }
    }
}
