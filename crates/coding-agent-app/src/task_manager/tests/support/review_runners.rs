#[cfg(feature = "test-support")]
#[async_trait::async_trait]
impl TaskRunner for DelayedCancellationRunner {
    async fn run(&self, mut context: RunContext, _sink: RunnerEventSink) -> RunnerOutcome {
        context.complete_preparation_for_test().await;
        self.started.notify_one();
        context.cancellation.cancelled().await;
        self.cancelled.notify_one();
        self.release.notified().await;
        RunnerOutcome::Cancelled
    }
}

#[cfg(feature = "test-support")]
#[async_trait::async_trait]
impl TaskRunner for StagedReviewStopRunner {
    async fn run(&self, mut context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
        context.complete_preparation_for_test().await;
        if sink
            .append(RunnerEvent::PlanUpdated(crate::fake_runner::fake_plan()))
            .await
            .is_err()
        {
            return RunnerOutcome::Failed(TaskFailure {
                code: "STAGED_REVIEW_PLAN_REJECTED".to_owned(),
                message: "the staged-review fixture plan was rejected".to_owned(),
                retryable: false,
            });
        }
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        self.review_release.notified().await;
        let review = sink.record_review(staged_review_evidence()).await;
        *self
            .review_result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(review);
        self.review_applied.notify_one();
        self.finish_release.notified().await;
        RunnerOutcome::Cancelled
    }
}

#[cfg(feature = "test-support")]
#[async_trait::async_trait]
impl TaskRunner for ConcurrentReviewRunner {
    async fn run(&self, mut context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
        context.complete_preparation_for_test().await;
        if sink
            .append(RunnerEvent::PlanUpdated(crate::fake_runner::fake_plan()))
            .await
            .is_err()
        {
            return RunnerOutcome::Failed(TaskFailure {
                code: "CONCURRENT_REVIEW_PLAN_REJECTED".to_owned(),
                message: "the concurrent-review fixture plan was rejected".to_owned(),
                retryable: false,
            });
        }
        self.started.notify_one();
        self.review_release.notified().await;
        let (first, second) = tokio::join!(
            sink.record_review(staged_review_evidence()),
            sink.record_review(staged_review_evidence()),
        );
        *self
            .review_results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((first, second));
        self.reviews_applied.notify_one();
        self.finish_release.notified().await;
        RunnerOutcome::Cancelled
    }
}

#[cfg(feature = "test-support")]
#[async_trait::async_trait]
impl TaskRunner for BidirectionalFifoRunner {
    async fn run(&self, mut context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
        context.complete_preparation_for_test().await;
        if sink
            .append(RunnerEvent::PlanUpdated(crate::fake_runner::fake_plan()))
            .await
            .is_err()
        {
            return RunnerOutcome::Failed(TaskFailure {
                code: "BIDIRECTIONAL_FIFO_PLAN_REJECTED".to_owned(),
                message: "the bidirectional FIFO fixture plan was rejected".to_owned(),
                retryable: false,
            });
        }
        self.started.notify_one();
        self.release.notified().await;
        let (review, event) = if self.review_first {
            tokio::join!(
                biased;
                sink.record_review(staged_review_evidence()),
                sink.append(RunnerEvent::PlanUpdated(crate::fake_runner::fake_plan())),
            )
        } else {
            let (event, review) = tokio::join!(
                biased;
                sink.append(RunnerEvent::PlanUpdated(crate::fake_runner::fake_plan())),
                sink.record_review(staged_review_evidence()),
            );
            (review, event)
        };
        *self
            .results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((review, event));
        self.completed.notify_one();
        self.finish_release.notified().await;
        RunnerOutcome::Cancelled
    }
}

#[cfg(feature = "test-support")]
#[async_trait::async_trait]
impl TaskRunner for GenericRecoveryLeaseRunner {
    async fn run(&self, mut context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
        context.complete_preparation_for_test().await;
        self.started.notify_one();
        self.event_release.notified().await;
        let result = sink
            .append(RunnerEvent::PlanUpdated(crate::fake_runner::fake_plan()))
            .await;
        *self
            .event_result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
        self.event_completed.notify_one();
        context.cancellation.cancelled().await;
        RunnerOutcome::Cancelled
    }
}
