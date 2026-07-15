use std::time::Duration;

use crate::{RunContext, RunnerEvent, RunnerEventSink, RunnerOutcome, TaskRunner};
use coding_agent_domain::{
    ActivityEntry, ActivityLevel, DiffFile, DiffFileStatus, DiffSnapshot, PlanItem, PlanItemStatus,
    PlanSnapshot, TaskFailure, TestCase, TestSnapshot, TestStatus, UtcTimestamp,
};

const DEFAULT_EMISSION_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FakeRunnerConfig {
    emission_interval: Duration,
}

impl FakeRunnerConfig {
    pub fn new(emission_interval: Duration) -> Self {
        assert!(
            !emission_interval.is_zero(),
            "fake runner emission interval must be positive"
        );
        Self { emission_interval }
    }

    pub const fn emission_interval(self) -> Duration {
        self.emission_interval
    }
}

impl Default for FakeRunnerConfig {
    fn default() -> Self {
        Self::new(DEFAULT_EMISSION_INTERVAL)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FakeTaskRunner {
    config: FakeRunnerConfig,
}

impl FakeTaskRunner {
    pub const fn new(config: FakeRunnerConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl TaskRunner for FakeTaskRunner {
    async fn run(&self, context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
        run_success(self.config, context, sink).await
    }
}

async fn run_success(
    config: FakeRunnerConfig,
    context: RunContext,
    sink: RunnerEventSink,
) -> RunnerOutcome {
    let activity_time = context.task.started_at.unwrap_or(context.task.created_at);
    let events = success_events(activity_time);
    let started_at = tokio::time::Instant::now();

    for (index, event) in events.into_iter().enumerate() {
        if context.cancellation.is_cancelled() {
            return RunnerOutcome::Cancelled;
        }
        let deadline = started_at
            + config
                .emission_interval()
                .saturating_mul(u32::try_from(index).unwrap_or(u32::MAX));
        tokio::select! {
            biased;
            () = context.cancellation.cancelled() => return RunnerOutcome::Cancelled,
            () = tokio::time::sleep_until(deadline) => {}
        }
        if context.cancellation.is_cancelled() {
            return RunnerOutcome::Cancelled;
        }
        if sink.append(event).await.is_err() {
            return if context.cancellation.is_cancelled() {
                RunnerOutcome::Cancelled
            } else {
                RunnerOutcome::Failed(event_rejected_failure())
            };
        }
    }

    if context.cancellation.is_cancelled() {
        RunnerOutcome::Cancelled
    } else {
        RunnerOutcome::Succeeded
    }
}

fn success_events(activity_time: UtcTimestamp) -> Vec<RunnerEvent> {
    vec![
        RunnerEvent::PlanUpdated(PlanSnapshot {
            revision: 1,
            items: vec![
                PlanItem {
                    id: "fake-plan".to_owned(),
                    title: "Prepare deterministic plan".to_owned(),
                    status: PlanItemStatus::Completed,
                },
                PlanItem {
                    id: "fake-diff".to_owned(),
                    title: "Generate synthetic diff".to_owned(),
                    status: PlanItemStatus::Completed,
                },
                PlanItem {
                    id: "fake-tests".to_owned(),
                    title: "Report synthetic tests".to_owned(),
                    status: PlanItemStatus::Completed,
                },
            ],
        }),
        RunnerEvent::ActivityAppended(activity(
            "fake-plan-ready",
            "Prepared deterministic plan",
            activity_time,
        )),
        RunnerEvent::ActivityAppended(activity(
            "fake-diff-ready",
            "Generated synthetic diff",
            activity_time,
        )),
        RunnerEvent::ActivityAppended(activity(
            "fake-tests-ready",
            "Started synthetic tests",
            activity_time,
        )),
        RunnerEvent::DiffUpdated(DiffSnapshot {
            revision: 1,
            files: vec![DiffFile {
                path: "synthetic/example.rs".to_owned(),
                status: DiffFileStatus::Added,
                patch: concat!(
                    "diff --git a/synthetic/example.rs b/synthetic/example.rs\n",
                    "new file mode 100644\n",
                    "--- /dev/null\n",
                    "+++ b/synthetic/example.rs\n",
                    "@@ -0,0 +1 @@\n",
                    "+// deterministic fake change\n",
                )
                .to_owned(),
                additions: 1,
                deletions: 0,
            }],
        }),
        RunnerEvent::TestUpdated(TestSnapshot {
            revision: 1,
            status: TestStatus::Running,
            cases: vec![TestCase {
                id: "fake-test".to_owned(),
                name: "deterministic synthetic check".to_owned(),
                status: TestStatus::Running,
                duration_ms: 0,
                summary: "Synthetic checks are running".to_owned(),
            }],
        }),
        RunnerEvent::TestUpdated(TestSnapshot {
            revision: 2,
            status: TestStatus::Passed,
            cases: vec![TestCase {
                id: "fake-test".to_owned(),
                name: "deterministic synthetic check".to_owned(),
                status: TestStatus::Passed,
                duration_ms: 200,
                summary: "Synthetic checks passed".to_owned(),
            }],
        }),
    ]
}

fn activity(id: &str, message: &str, created_at: UtcTimestamp) -> ActivityEntry {
    ActivityEntry {
        id: id.to_owned(),
        level: ActivityLevel::Info,
        message: message.to_owned(),
        created_at,
    }
}

fn event_rejected_failure() -> TaskFailure {
    TaskFailure {
        code: "FAKE_RUNNER_EVENT_REJECTED".to_owned(),
        message: "fake runner event was rejected".to_owned(),
        retryable: true,
    }
}

#[cfg(feature = "test-support")]
mod scripted {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use coding_agent_domain::TaskId;
    use tokio::sync::Notify;

    use super::{FakeRunnerConfig, run_success};
    use crate::{RunContext, RunnerEventSink, RunnerOutcome, TaskRunner};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum FakeScenario {
        Success,
        Blocking,
        IgnoresCancellation,
        Failure,
        Panic,
    }

    pub struct ScriptedFakeRunner {
        config: FakeRunnerConfig,
        scenarios: Mutex<VecDeque<FakeScenario>>,
        started: Mutex<Vec<TaskId>>,
        releases: Mutex<HashMap<TaskId, Arc<Notify>>>,
    }

    impl ScriptedFakeRunner {
        pub fn new(
            config: FakeRunnerConfig,
            scenarios: impl IntoIterator<Item = FakeScenario>,
        ) -> Self {
            Self {
                config,
                scenarios: Mutex::new(scenarios.into_iter().collect()),
                started: Mutex::new(Vec::new()),
                releases: Mutex::new(HashMap::new()),
            }
        }

        pub fn release(&self, task_id: TaskId) -> bool {
            let release = self
                .releases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&task_id);
            if let Some(release) = release {
                release.notify_one();
                true
            } else {
                false
            }
        }

        pub fn started_task_ids(&self) -> Vec<TaskId> {
            self.started
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn next_scenario(&self) -> FakeScenario {
            self.scenarios
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .expect("a scripted fake scenario is required for every task")
        }

        fn install_release(&self, task_id: TaskId) -> Arc<Notify> {
            let release = Arc::new(Notify::new());
            self.releases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(task_id, release.clone());
            release
        }

        fn record_start(&self, task_id: TaskId) {
            self.started
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(task_id);
        }

        fn remove_release(&self, task_id: TaskId) {
            self.releases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&task_id);
        }
    }

    #[async_trait::async_trait]
    impl TaskRunner for ScriptedFakeRunner {
        async fn run(&self, context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
            let task_id = context.task.id;
            let scenario = self.next_scenario();
            let release = match scenario {
                FakeScenario::Blocking | FakeScenario::IgnoresCancellation => {
                    Some(self.install_release(task_id))
                }
                FakeScenario::Success | FakeScenario::Failure | FakeScenario::Panic => None,
            };
            self.record_start(task_id);

            match scenario {
                FakeScenario::Success => run_success(self.config, context, sink).await,
                FakeScenario::Blocking => {
                    let release = release.expect("blocking scenario installs a release");
                    let outcome = tokio::select! {
                        () = context.cancellation.cancelled() => RunnerOutcome::Cancelled,
                        () = release.notified() => RunnerOutcome::Succeeded,
                    };
                    self.remove_release(task_id);
                    outcome
                }
                FakeScenario::IgnoresCancellation => {
                    release
                        .expect("ignore-cancellation scenario installs a release")
                        .notified()
                        .await;
                    self.remove_release(task_id);
                    RunnerOutcome::Succeeded
                }
                FakeScenario::Failure => RunnerOutcome::Failed(fixed_failure()),
                FakeScenario::Panic => panic!("injected scripted fake runner panic"),
            }
        }
    }

    fn fixed_failure() -> coding_agent_domain::TaskFailure {
        coding_agent_domain::TaskFailure {
            code: "FAKE_RUNNER_FAILURE".to_owned(),
            message: "deterministic fake runner failure".to_owned(),
            retryable: true,
        }
    }
}

#[cfg(feature = "test-support")]
pub use scripted::{FakeScenario, ScriptedFakeRunner};
