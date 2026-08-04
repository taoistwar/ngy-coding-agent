use super::*;

impl TaskManager {
    pub(super) fn finalize_terminal_release_commit(
        &mut self,
        commit: TerminalReleaseCommit,
    ) -> Vec<RunnerShutdownHandle> {
        let expected_count = commit.released_count();
        let (committed, shutdown_handles) = commit.into_parts();
        debug_assert_eq!(committed.len(), expected_count);
        for committed in committed {
            let task_id = committed.task_id();
            debug_assert!(!self.active.contains_key(&task_id));
            let active = committed.into_active();
            debug_assert_eq!(
                active.permit.state(),
                Ok(crate::PermitOwnershipState::Released)
            );
            debug_assert!(
                active
                    .cleanup_confirmation
                    .as_ref()
                    .is_some_and(|cleanup| !cleanup.is_available_for_terminal_release())
            );
            debug_assert!(active.done_sender.is_none());
            #[cfg(test)]
            if let Some(hooks) = &self.claim_hooks {
                hooks
                    .active_count
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        shutdown_handles
    }

    pub(super) fn take_shutdown_handles(&mut self) -> Vec<RunnerShutdownHandle> {
        let mut task_ids = self.active.keys().copied().collect::<Vec<_>>();
        task_ids.sort_by_key(ToString::to_string);
        task_ids
            .into_iter()
            .filter_map(|task_id| {
                let active = self.active.get_mut(&task_id)?;
                Some(RunnerShutdownHandle {
                    task_id,
                    cancellation: active.cancellation.clone(),
                    done: active.done_receiver.take()?,
                })
            })
            .collect()
    }

    pub(super) fn remove_active(&mut self, task_id: TaskId) {
        if let Some(mut active) = self.active.remove(&task_id) {
            #[cfg(test)]
            if let Some(hooks) = &self.claim_hooks {
                hooks
                    .active_count
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
            if let Some(done) = active.done_sender.take() {
                let _ = done.send(());
            }
        }
    }
}
