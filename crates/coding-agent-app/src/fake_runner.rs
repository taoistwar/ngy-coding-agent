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
                truncated: false,
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
    use std::cmp::Ordering;
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use coding_agent_domain::TaskId;
    use tokio::sync::{Notify, watch};

    use super::{FakeRunnerConfig, run_success};
    use crate::{RunContext, RunnerEventSink, RunnerOutcome, TaskRunner};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum FakeScenario {
        Success,
        Blocking,
        IgnoresCancellation,
        Failure,
        Panic,
    }

    pub struct ScriptedFakeRunner {
        config: FakeRunnerConfig,
        scenario_state: Mutex<ScenarioState>,
        next_ordinal: watch::Sender<u64>,
        started: Mutex<Vec<TaskId>>,
        started_count: watch::Sender<usize>,
        releases: Mutex<HashMap<TaskId, Arc<Notify>>>,
    }

    struct ScenarioState {
        scenarios: VecDeque<FakeScenario>,
        next_ordinal: u64,
    }

    impl ScriptedFakeRunner {
        pub fn new(
            config: FakeRunnerConfig,
            scenarios: impl IntoIterator<Item = FakeScenario>,
        ) -> Self {
            let (next_ordinal, _) = watch::channel(0);
            let (started_count, _) = watch::channel(0);
            Self {
                config,
                scenario_state: Mutex::new(ScenarioState {
                    scenarios: scenarios.into_iter().collect(),
                    next_ordinal: 0,
                }),
                next_ordinal,
                started: Mutex::new(Vec::new()),
                started_count,
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

        pub fn release_next(&self) -> Option<TaskId> {
            let started = self.started_task_ids();
            let released = {
                let mut releases = self
                    .releases
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                started
                    .into_iter()
                    .find_map(|task_id| releases.remove(&task_id).map(|release| (task_id, release)))
            };
            released.map(|(task_id, release)| {
                release.notify_one();
                task_id
            })
        }

        pub async fn wait_and_release_next(&self) -> TaskId {
            let mut started_count = self.started_count.subscribe();
            loop {
                if let Some(task_id) = self.release_next() {
                    return task_id;
                }
                started_count
                    .changed()
                    .await
                    .expect("scripted fake runner remains alive while waiting for a task");
            }
        }

        async fn scenario_for(&self, launch_ordinal: u64) -> FakeScenario {
            let mut next_ordinal = self.next_ordinal.subscribe();
            loop {
                let assigned = {
                    let mut state = self
                        .scenario_state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    match launch_ordinal.cmp(&state.next_ordinal) {
                        Ordering::Equal => {
                            let scenario = state
                                .scenarios
                                .pop_front()
                                .expect("a scripted fake scenario is required for every task");
                            state.next_ordinal = state
                                .next_ordinal
                                .checked_add(1)
                                .expect("scripted fake launch ordinal overflow");
                            Some((scenario, state.next_ordinal))
                        }
                        Ordering::Greater => None,
                        Ordering::Less => {
                            panic!("scripted fake task launch ordinal was already consumed")
                        }
                    }
                };
                if let Some((scenario, next)) = assigned {
                    self.next_ordinal.send_replace(next);
                    return scenario;
                }
                next_ordinal
                    .changed()
                    .await
                    .expect("scripted fake ordinal sender remains alive");
            }
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
            let started_count = {
                let mut started = self
                    .started
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                started.push(task_id);
                started.len()
            };
            self.started_count.send_replace(started_count);
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
            let scenario = self.scenario_for(context.launch_ordinal()).await;
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

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;

        use coding_agent_domain::TaskId;

        use super::ScriptedFakeRunner;
        use crate::FakeRunnerConfig;

        #[tokio::test]
        async fn virtual_release_waits_for_and_releases_the_next_started_task() {
            let runner = Arc::new(ScriptedFakeRunner::new(FakeRunnerConfig::default(), []));
            let release = tokio::spawn({
                let runner = runner.clone();
                async move { runner.wait_and_release_next().await }
            });
            tokio::task::yield_now().await;
            assert!(!release.is_finished());

            let task_id = TaskId::new();
            let task_release = runner.install_release(task_id);
            runner.record_start(task_id);

            assert_eq!(release.await.expect("join release waiter"), task_id);
            tokio::time::timeout(std::time::Duration::from_secs(1), task_release.notified())
                .await
                .expect("release signal is delivered");
        }
    }
}

#[cfg(feature = "test-support")]
pub use scripted::{FakeScenario, ScriptedFakeRunner};

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use super::FakeScenario;

    #[test]
    fn scripted_scenarios_use_a_closed_snake_case_schema() {
        assert_eq!(
            serde_json::from_str::<FakeScenario>(r#""ignores_cancellation""#)
                .expect("deserialize scripted scenario"),
            FakeScenario::IgnoresCancellation
        );
        assert!(serde_json::from_str::<FakeScenario>(r#""prompt_failure""#).is_err());
    }
}
