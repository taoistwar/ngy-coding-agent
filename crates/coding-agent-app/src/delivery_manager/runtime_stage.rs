use std::future::Future;
use std::time::Duration;

use tokio::time::timeout;

pub(super) enum ProcessStageCompletion<T, E> {
    Completed(Result<T, E>),
    TimedOutWithCleanupUnproven,
}

pub(super) async fn run_process_stage<T, E>(
    stage_timeout: Duration,
    future: impl Future<Output = Result<T, E>>,
) -> ProcessStageCompletion<T, E> {
    match timeout(stage_timeout, future).await {
        Ok(result) => ProcessStageCompletion::Completed(result),
        Err(_) => ProcessStageCompletion::TimedOutWithCleanupUnproven,
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn preserves_completed_success_and_error() {
        let success = run_process_stage(Duration::from_secs(1), async {
            Ok::<_, &'static str>("success")
        })
        .await;
        assert!(matches!(
            success,
            ProcessStageCompletion::Completed(Ok("success"))
        ));

        let error =
            run_process_stage(Duration::from_secs(1), async { Err::<(), _>("error") }).await;
        assert!(matches!(
            error,
            ProcessStageCompletion::Completed(Err("error"))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_drops_the_runtime_future_before_returning() {
        struct DropProof(Arc<AtomicBool>);

        impl Drop for DropProof {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let future_drop = DropProof(dropped.clone());
        let outcome = run_process_stage(Duration::from_secs(1), async move {
            let _future_drop = future_drop;
            pending::<Result<(), ()>>().await
        })
        .await;

        assert!(matches!(
            outcome,
            ProcessStageCompletion::TimedOutWithCleanupUnproven
        ));
        assert!(
            dropped.load(Ordering::SeqCst),
            "runtime future must be dropped before timeout classification is returned"
        );
    }
}
