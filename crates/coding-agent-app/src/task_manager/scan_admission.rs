use super::*;

impl TaskManager {
    pub(super) async fn handle_scan_snapshot(
        &mut self,
        scan_generation: u64,
        snapshot: SchedulerBootstrapSnapshot,
    ) {
        if let Err(error) = self.dispatcher.flush_to(snapshot.membership_event_id).await {
            tracing::error!(%error, "scheduler scan event projection failed");
            self.finish_scan();
            self.freeze_degraded();
            return;
        }
        match self.publish_scheduler_snapshot(&snapshot) {
            Ok(_) => {}
            Err(SchedulerProjectionPublishError::StaleSnapshot) => {
                self.finish_scan();
                self.scan_requested = self.claims_allowed();
                return;
            }
            Err(error) => {
                tracing::error!(%error, "scheduler scan projection publication failed");
                self.finish_scan();
                self.freeze_degraded();
                return;
            }
        }
        if !self.claims_allowed() {
            self.finish_scan();
            return;
        }
        let repositories = snapshot
            .repositories
            .into_iter()
            .map(|repository| (repository.id, repository))
            .collect::<HashMap<_, _>>();
        self.scan_available.clear();
        for task in snapshot
            .tasks
            .into_iter()
            .filter(|task| task.status == TaskStatus::Queued && !self.active.contains_key(&task.id))
        {
            let Some(repository) = repositories.get(&task.repository_id).cloned() else {
                tracing::error!(task_id = %task.id, "queued task references a missing repository");
                continue;
            };
            let Ok(key) = self.repository_control.coordination_key(repository.id) else {
                tracing::warn!(task_id = %task.id, "repository has no registered control identity");
                continue;
            };
            self.scan_available.insert(task.id, (task, repository, key));
        }
        self.scan_generation = scan_generation;
        self.scan_gates = SchedulerAdmissionGates::default();
        self.start_next_storage_admission();
    }

    pub(super) fn start_next_storage_admission(&mut self) {
        if !self.scan_in_flight
            || self.storage_admission_in_flight.is_some()
            || !self.claims_allowed()
        {
            if !self.claims_allowed() {
                self.finish_scan();
            }
            return;
        }
        if !self.scan_available.is_empty() {
            let candidates = self
                .scan_available
                .values()
                .map(|(task, _, key)| {
                    QueuedTaskCandidate::new(task.id, task.repository_id, *key, task.created_at)
                })
                .collect::<Vec<_>>();
            let Ok(scan) = scan_queued_candidates(
                &candidates,
                &self.permit_ledger.snapshot(),
                &self.scan_gates,
            ) else {
                self.freeze_degraded();
                return;
            };
            let Some(candidate) = scan.next_candidate else {
                self.finish_scan();
                return;
            };
            let Some((task, repository, coordination_key)) =
                self.scan_available.remove(&candidate.task_id())
            else {
                self.freeze_degraded();
                return;
            };
            let operation_nonce = self.next_operation_nonce;
            self.next_operation_nonce = match operation_nonce.checked_add(1) {
                Some(next) => next,
                None => {
                    self.freeze_degraded();
                    return;
                }
            };
            let admitted = StorageAdmissionCandidate {
                scan_generation: self.scan_generation,
                operation_nonce,
                task,
                repository,
                coordination_key,
            };
            self.storage_admission_in_flight = Some(admitted.clone());
            let storage_admission = self.storage_admission.clone();
            let store = self.store.clone();
            let completion_sender = self.completion_sender.clone();
            let active_task_count = self.permit_ledger.snapshot().global_owned();
            tokio::spawn(async move {
                let storage_ready = storage_admission
                    .refresh_for_repository_admission(
                        active_task_count,
                        admitted.task.repository_id,
                    )
                    .await;
                let result = match storage_ready {
                    Ok(RefreshedStorageAdmission::RepositoryBlocked) => {
                        StorageAdmissionResult::RepositoryBlocked
                    }
                    Ok(RefreshedStorageAdmission::GlobalBlocked) => {
                        StorageAdmissionResult::GlobalBlocked
                    }
                    Err(StorageMonitorError::UnknownRepositoryScope) => {
                        StorageAdmissionResult::RepositoryBlocked
                    }
                    Err(error) => {
                        tracing::warn!(
                            task_id = %admitted.task.id,
                            repository_id = %admitted.task.repository_id,
                            %error,
                            "storage admission refresh failed"
                        );
                        StorageAdmissionResult::Unavailable
                    }
                    Ok(RefreshedStorageAdmission::Ready) => {
                        match store.task_detail(admitted.task.id).await {
                            Ok(Some(detail)) if detail.task == admitted.task => {
                                StorageAdmissionResult::Ready
                            }
                            Ok(_) => StorageAdmissionResult::Stale,
                            Err(_) => StorageAdmissionResult::Unavailable,
                        }
                    }
                };
                let _ = completion_sender
                    .send(TaskManagerCompletion::StorageAdmissionCompleted {
                        scan_generation: admitted.scan_generation,
                        task_id: admitted.task.id,
                        operation_nonce: admitted.operation_nonce,
                        result,
                    })
                    .await;
            });
            return;
        }
        self.finish_scan();
    }

    pub(super) async fn handle_storage_admission_completed(
        &mut self,
        scan_generation: u64,
        task_id: TaskId,
        operation_nonce: u64,
        result: StorageAdmissionResult,
    ) {
        let is_current = self
            .storage_admission_in_flight
            .as_ref()
            .is_some_and(|candidate| {
                candidate.scan_generation == scan_generation
                    && candidate.task.id == task_id
                    && candidate.operation_nonce == operation_nonce
            });
        if !is_current {
            return;
        }
        let admitted = self
            .storage_admission_in_flight
            .take()
            .expect("the exact storage admission was checked above");
        match result {
            StorageAdmissionResult::Ready if self.claims_allowed() => {
                self.claim_ready_candidate(admitted).await;
            }
            StorageAdmissionResult::RepositoryBlocked => {
                self.scan_gates.set_storage_pressure(admitted.task.id, true);
                self.scan_available.insert(
                    admitted.task.id,
                    (
                        admitted.task,
                        admitted.repository,
                        admitted.coordination_key,
                    ),
                );
                self.start_next_storage_admission();
            }
            StorageAdmissionResult::GlobalBlocked => {
                self.finish_scan();
            }
            StorageAdmissionResult::Stale => {
                self.scan_requested = true;
                self.finish_scan();
            }
            StorageAdmissionResult::Unavailable => {
                tracing::warn!(%task_id, "storage admission refresh was unavailable");
                self.finish_scan();
            }
            StorageAdmissionResult::Ready => {
                self.finish_scan();
            }
        }
    }

    pub(super) fn finish_scan(&mut self) {
        self.scan_in_flight = false;
        self.scan_available.clear();
        self.storage_admission_in_flight = None;
    }
}
