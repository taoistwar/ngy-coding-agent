use std::time::Duration;

use crate::{RunContext, RunnerEvent, RunnerEventSink, RunnerOutcome, TaskRunner};
#[cfg(feature = "test-support")]
use coding_agent_domain::{ActivityActor, FindingSeverity, ReviewFinding};
use coding_agent_domain::{
    ActivityEntry, ActivityLevel, CheckActor, CheckEvidence, CheckEvidenceStatus, DiffFile,
    DiffFileStatus, DiffSnapshot, NewReviewEvidence, PlanItem, PlanItemStatus, PlanSnapshot,
    RequiredCheck, ReviewCoverageEvidence, ReviewDecisionSource, ReviewVerdict, TaskFailure,
    TestCase, TestSnapshot, TestStatus, UtcTimestamp, WorkspaceDigest,
};

const DEFAULT_EMISSION_INTERVAL: Duration = Duration::from_millis(200);
const APPROVED_WORKSPACE_GENERATION: u64 = 1;

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
        RunnerOutcome::Approved(approved_evidence())
    }
}

pub(crate) fn approved_evidence() -> NewReviewEvidence {
    let generation = APPROVED_WORKSPACE_GENERATION;
    let digest = WorkspaceDigest::try_new("a".repeat(64))
        .expect("fake runner uses a valid workspace digest");
    let check = fake_required_check();
    let check_evidence = CheckEvidence::try_for_check(
        &check,
        CheckActor::Executor,
        1,
        generation,
        digest.clone(),
        CheckEvidenceStatus::Passed,
        200,
        "deterministic synthetic check passed",
        false,
    )
    .expect("fake runner uses valid check evidence");
    let coverage =
        ReviewCoverageEvidence::try_new(generation, digest.clone(), "f".repeat(64), vec![0], 1)
            .expect("fake runner uses valid review coverage");
    NewReviewEvidence::try_new(
        1,
        ReviewDecisionSource::Reviewer,
        generation,
        digest,
        ReviewVerdict::Approved,
        "deterministic synthetic review approved",
        Vec::new(),
        Vec::new(),
        vec![check],
        vec![check_evidence],
        Some(coverage),
    )
    .expect("fake runner uses valid approved evidence")
}

fn fake_required_check() -> RequiredCheck {
    RequiredCheck::try_cargo_test("fake-cargo-test", None, None)
        .expect("fake runner uses a valid required check")
}

pub(crate) fn fake_plan() -> PlanSnapshot {
    PlanSnapshot::try_structured(
        1,
        "Prepare deterministic synthetic output",
        vec![
            PlanItem::try_structured(
                "fake-plan",
                "Prepare deterministic plan",
                "Prepare the deterministic fixture plan",
                vec!["The fixture plan is available".to_owned()],
                PlanItemStatus::Completed,
            )
            .expect("fake runner uses a valid plan item"),
            PlanItem::try_structured(
                "fake-diff",
                "Generate synthetic diff",
                "Generate the deterministic fixture diff",
                vec!["The fixture diff is available".to_owned()],
                PlanItemStatus::Completed,
            )
            .expect("fake runner uses a valid plan item"),
            PlanItem::try_structured(
                "fake-tests",
                "Report synthetic tests",
                "Report the deterministic fixture test result",
                vec!["The fixture check passes".to_owned()],
                PlanItemStatus::Completed,
            )
            .expect("fake runner uses a valid plan item"),
        ],
        vec![fake_required_check()],
    )
    .expect("fake runner uses a valid structured plan")
}

#[cfg(feature = "test-support")]
async fn approved_after_panel_barrier(sink: &RunnerEventSink) -> RunnerOutcome {
    for event in [
        RunnerEvent::PlanUpdated(fake_plan()),
        RunnerEvent::DiffUpdated(approved_diff()),
        RunnerEvent::TestUpdated(approved_tests(TestStatus::Passed)),
    ] {
        if sink.append(event).await.is_err() {
            return RunnerOutcome::Failed(event_rejected_failure());
        }
    }
    RunnerOutcome::Approved(approved_evidence())
}

fn success_events(activity_time: UtcTimestamp) -> Vec<RunnerEvent> {
    vec![
        RunnerEvent::PlanUpdated(fake_plan()),
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
        RunnerEvent::DiffUpdated(approved_diff()),
        RunnerEvent::TestUpdated(approved_tests(TestStatus::Running)),
        RunnerEvent::TestUpdated(approved_tests(TestStatus::Passed)),
    ]
}

fn approved_diff() -> DiffSnapshot {
    DiffSnapshot {
        revision: APPROVED_WORKSPACE_GENERATION,
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
    }
}

fn approved_tests(status: TestStatus) -> TestSnapshot {
    let (duration_ms, summary) = match status {
        TestStatus::Running => (0, "Synthetic checks are running"),
        TestStatus::Passed => (200, "Synthetic checks passed"),
        TestStatus::Queued | TestStatus::Failed | TestStatus::Cancelled => {
            panic!("fake approved test fixture supports only running and passed")
        }
    };
    TestSnapshot {
        revision: APPROVED_WORKSPACE_GENERATION,
        status,
        cases: vec![TestCase {
            id: "fake-test".to_owned(),
            name: "deterministic synthetic check".to_owned(),
            status,
            duration_ms,
            summary: summary.to_owned(),
        }],
    }
}

fn activity(id: &str, message: &str, created_at: UtcTimestamp) -> ActivityEntry {
    ActivityEntry::legacy(id, ActivityLevel::Info, message, created_at)
}

fn event_rejected_failure() -> TaskFailure {
    TaskFailure {
        code: "FAKE_RUNNER_EVENT_REJECTED".to_owned(),
        message: "fake runner event was rejected".to_owned(),
        retryable: true,
    }
}

#[cfg(feature = "test-support")]
async fn run_multi_role_scenario(
    context: RunContext,
    sink: RunnerEventSink,
    final_round: u8,
    approved: bool,
) -> RunnerOutcome {
    let created_at = context.task.started_at.unwrap_or(context.task.created_at);
    if append_process_event(&context, &sink, RunnerEvent::PlanUpdated(fake_plan()))
        .await
        .is_err()
    {
        return cancelled_or_rejected(&context);
    }
    if append_process_event(
        &context,
        &sink,
        RunnerEvent::ActivityAppended(role_activity(
            "planner-1-plan",
            ActivityActor::Planner,
            1,
            "Planner submitted the structured implementation plan",
            created_at,
        )),
    )
    .await
    .is_err()
    {
        return cancelled_or_rejected(&context);
    }

    for round in 1..=final_round {
        let events = [
            RunnerEvent::ActivityAppended(role_activity(
                &format!("executor-{round}-implementation"),
                ActivityActor::Executor,
                u32::from(round),
                &format!("Executor #{round} prepared workspace generation {round}"),
                created_at,
            )),
            RunnerEvent::DiffUpdated(generation_diff(u64::from(round))),
            RunnerEvent::TestUpdated(generation_tests(u64::from(round), round)),
            RunnerEvent::ActivityAppended(role_activity(
                &format!("reviewer-{round}-review"),
                ActivityActor::Reviewer,
                u32::from(round),
                &format!("Reviewer #{round} inspected the complete bounded diff"),
                created_at,
            )),
        ];
        for event in events {
            if append_process_event(&context, &sink, event).await.is_err() {
                return cancelled_or_rejected(&context);
            }
        }

        if round < final_round
            && sink
                .record_review(changes_requested_evidence(round))
                .await
                .is_err()
        {
            return cancelled_or_rejected(&context);
        }
    }

    if context.cancellation.is_cancelled() {
        RunnerOutcome::Cancelled
    } else if approved {
        RunnerOutcome::Approved(approved_evidence_for_round(final_round))
    } else {
        RunnerOutcome::Rejected(changes_requested_evidence(final_round))
    }
}

#[cfg(feature = "test-support")]
async fn append_process_event(
    context: &RunContext,
    sink: &RunnerEventSink,
    event: RunnerEvent,
) -> Result<(), ()> {
    if context.cancellation.is_cancelled() {
        return Err(());
    }
    sink.append(event).await.map(|_| ()).map_err(|_| ())
}

#[cfg(feature = "test-support")]
fn cancelled_or_rejected(context: &RunContext) -> RunnerOutcome {
    if context.cancellation.is_cancelled() {
        RunnerOutcome::Cancelled
    } else {
        RunnerOutcome::Failed(event_rejected_failure())
    }
}

#[cfg(feature = "test-support")]
fn role_activity(
    id: &str,
    actor: ActivityActor,
    role_run: u32,
    message: &str,
    created_at: UtcTimestamp,
) -> ActivityEntry {
    ActivityEntry::try_new(
        id,
        ActivityLevel::Info,
        actor,
        Some(role_run),
        message,
        created_at,
    )
    .expect("process fake runner uses valid role activity")
}

#[cfg(feature = "test-support")]
fn generation_diff(generation: u64) -> DiffSnapshot {
    DiffSnapshot {
        revision: generation,
        files: vec![DiffFile {
            path: "synthetic/example.rs".to_owned(),
            status: DiffFileStatus::Modified,
            patch: format!(
                "diff --git a/synthetic/example.rs b/synthetic/example.rs\n\
                 --- a/synthetic/example.rs\n\
                 +++ b/synthetic/example.rs\n\
                 @@ -1 +1 @@\n\
                 -// previous generation\n\
                 +// deterministic workspace generation {generation}\n"
            ),
            additions: 1,
            deletions: 1,
            truncated: false,
        }],
    }
}

#[cfg(feature = "test-support")]
fn generation_tests(generation: u64, role_run: u8) -> TestSnapshot {
    TestSnapshot {
        revision: generation,
        status: TestStatus::Passed,
        cases: vec![TestCase {
            id: "fake-cargo-test".to_owned(),
            name: "cargo test".to_owned(),
            status: TestStatus::Passed,
            duration_ms: 200,
            summary: format!("Executor #{role_run} passed the required offline test"),
        }],
    }
}

#[cfg(feature = "test-support")]
fn checkpoint_evidence(round: u8) -> (WorkspaceDigest, RequiredCheck, CheckEvidence) {
    let generation = u64::from(round);
    let digest = WorkspaceDigest::try_new(
        char::from(b'a' + round.saturating_sub(1))
            .to_string()
            .repeat(64),
    )
    .expect("process fake runner uses a valid workspace digest");
    let check = fake_required_check();
    let evidence = CheckEvidence::try_for_check(
        &check,
        CheckActor::Executor,
        u32::from(round),
        generation,
        digest.clone(),
        CheckEvidenceStatus::Passed,
        200,
        format!("Executor #{round} passed cargo test for generation {generation}"),
        false,
    )
    .expect("process fake runner uses valid check evidence");
    (digest, check, evidence)
}

#[cfg(feature = "test-support")]
fn approved_evidence_for_round(round: u8) -> NewReviewEvidence {
    let generation = u64::from(round);
    let (digest, check, evidence) = checkpoint_evidence(round);
    let coverage = ReviewCoverageEvidence::try_new(
        generation,
        digest.clone(),
        char::from(b'f' - round.saturating_sub(1))
            .to_string()
            .repeat(64),
        vec![0],
        1,
    )
    .expect("process fake runner uses valid complete coverage");
    NewReviewEvidence::try_new(
        round,
        ReviewDecisionSource::Reviewer,
        generation,
        digest,
        ReviewVerdict::Approved,
        format!("Reviewer round {round} approved the complete current diff"),
        Vec::new(),
        Vec::new(),
        vec![check],
        vec![evidence],
        Some(coverage),
    )
    .expect("process fake runner uses valid approved evidence")
}

#[cfg(feature = "test-support")]
fn changes_requested_evidence(round: u8) -> NewReviewEvidence {
    let generation = u64::from(round);
    let (digest, check, evidence) = checkpoint_evidence(round);
    let finding = ReviewFinding::try_for_review(
        round,
        1,
        FindingSeverity::Blocking,
        format!("Reviewer round {round} requests one bounded correction"),
        Some("synthetic/example.rs".to_owned()),
        Some(1),
    )
    .expect("process fake runner uses a valid blocking finding");
    NewReviewEvidence::try_new(
        round,
        ReviewDecisionSource::Reviewer,
        generation,
        digest,
        ReviewVerdict::ChangesRequested,
        format!("Reviewer round {round} requested changes"),
        vec![finding],
        Vec::new(),
        vec![check],
        vec![evidence],
        None,
    )
    .expect("process fake runner uses valid changes-requested evidence")
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
        MultiRoleApproved,
        MultiRoleReworkApproved,
        MultiRoleRejected,
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
                FakeScenario::Success
                | FakeScenario::MultiRoleApproved
                | FakeScenario::MultiRoleReworkApproved
                | FakeScenario::MultiRoleRejected
                | FakeScenario::Failure
                | FakeScenario::Panic => None,
            };
            self.record_start(task_id);

            match scenario {
                FakeScenario::Success => run_success(self.config, context, sink).await,
                FakeScenario::MultiRoleApproved => {
                    super::run_multi_role_scenario(context, sink, 1, true).await
                }
                FakeScenario::MultiRoleReworkApproved => {
                    super::run_multi_role_scenario(context, sink, 2, true).await
                }
                FakeScenario::MultiRoleRejected => {
                    super::run_multi_role_scenario(context, sink, 3, false).await
                }
                FakeScenario::Blocking => {
                    let release = release.expect("blocking scenario installs a release");
                    let outcome = tokio::select! {
                        () = context.cancellation.cancelled() => RunnerOutcome::Cancelled,
                        () = release.notified() => super::approved_after_panel_barrier(&sink).await,
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
                    super::approved_after_panel_barrier(&sink).await
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
        assert_eq!(
            serde_json::from_str::<FakeScenario>(r#""multi_role_rework_approved""#)
                .expect("deserialize multi-role process scenario"),
            FakeScenario::MultiRoleReworkApproved
        );
    }
}
