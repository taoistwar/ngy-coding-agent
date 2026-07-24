use std::collections::HashSet;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

use crate::{
    DomainError, EventId, MAX_WORKSPACE_GENERATION, RequiredCheck, RequiredCheckSelector,
    ReviewEvidence, Task, TaskFailure, TaskId, UtcTimestamp,
};

const MAX_PLAN_BYTES: usize = 64 * 1024;
const MAX_PLAN_ITEMS: usize = 32;
const MAX_PLAN_SUMMARY_SCALARS: usize = 4_096;
const MAX_PLAN_TITLE_SCALARS: usize = 256;
const MAX_PLAN_DESCRIPTION_SCALARS: usize = 4_096;
const MAX_ACCEPTANCE_CRITERIA: usize = 8;
const MAX_ACCEPTANCE_CRITERION_SCALARS: usize = 1_024;
const MAX_INITIAL_REQUIRED_CHECKS: usize = 16;

#[derive(Default)]
enum LegacyField<T> {
    #[default]
    Missing,
    Present(T),
}

impl<T> LegacyField<T> {
    fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    fn into_present(self) -> Result<T, DomainError> {
        match self {
            Self::Missing => Err(DomainError::InvalidPlan),
            Self::Present(value) => Ok(value),
        }
    }
}

impl<'de, T> Deserialize<'de> for LegacyField<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::Present)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PlanSnapshot {
    format_version: u8,
    revision: u64,
    summary: String,
    items: Vec<PlanItem>,
    initial_required_checks: Vec<RequiredCheck>,
}

impl PlanSnapshot {
    pub fn legacy(revision: u64, items: Vec<PlanItem>) -> Self {
        Self {
            format_version: 0,
            revision,
            summary: String::new(),
            items: items.into_iter().map(PlanItem::into_legacy).collect(),
            initial_required_checks: Vec::new(),
        }
    }

    pub fn try_structured(
        revision: u64,
        summary: impl Into<String>,
        items: Vec<PlanItem>,
        initial_required_checks: Vec<RequiredCheck>,
    ) -> Result<Self, DomainError> {
        let plan = Self {
            format_version: 1,
            revision,
            summary: summary.into(),
            items,
            initial_required_checks,
        };
        plan.validate()?;
        Ok(plan)
    }

    pub const fn format_version(&self) -> u8 {
        self.format_version
    }

    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn items(&self) -> &[PlanItem] {
        &self.items
    }

    pub fn initial_required_checks(&self) -> &[RequiredCheck] {
        &self.initial_required_checks
    }

    pub fn into_parts(self) -> (u8, u64, String, Vec<PlanItem>, Vec<RequiredCheck>) {
        (
            self.format_version,
            self.revision,
            self.summary,
            self.items,
            self.initial_required_checks,
        )
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        match self.format_version {
            0 => {
                if !self.summary.is_empty()
                    || !self.initial_required_checks.is_empty()
                    || self.items.iter().any(|item| {
                        !item.description.is_empty() || !item.acceptance_criteria.is_empty()
                    })
                {
                    return Err(DomainError::InvalidPlan);
                }
            }
            1 => self.validate_structured()?,
            _ => return Err(DomainError::InvalidPlan),
        }
        Ok(())
    }

    fn validate_structured(&self) -> Result<(), DomainError> {
        if !(1..=MAX_WORKSPACE_GENERATION).contains(&self.revision)
            || self.summary.chars().count() > MAX_PLAN_SUMMARY_SCALARS
            || !(1..=MAX_PLAN_ITEMS).contains(&self.items.len())
            || !(1..=MAX_INITIAL_REQUIRED_CHECKS).contains(&self.initial_required_checks.len())
            || self
                .items
                .iter()
                .filter(|item| item.status == PlanItemStatus::Running)
                .count()
                > 1
        {
            return Err(DomainError::InvalidPlan);
        }

        let mut item_ids = HashSet::new();
        for item in &self.items {
            item.validate_structured()?;
            if !item_ids.insert(item.id.as_str()) {
                return Err(DomainError::InvalidPlan);
            }
        }

        let mut check_ids = HashSet::new();
        let mut selectors = HashSet::<&RequiredCheckSelector>::new();
        for check in &self.initial_required_checks {
            if !check_ids.insert(check.id()) || !selectors.insert(check.selector()) {
                return Err(DomainError::InvalidPlan);
            }
        }
        if !self
            .initial_required_checks
            .iter()
            .any(RequiredCheck::is_cargo_test)
            || serde_json::to_vec(self)
                .map_err(|_| DomainError::InvalidPlan)?
                .len()
                > MAX_PLAN_BYTES
        {
            return Err(DomainError::InvalidPlan);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPlanSnapshot {
    #[serde(default)]
    format_version: LegacyField<u8>,
    revision: u64,
    #[serde(default)]
    summary: LegacyField<String>,
    items: Vec<RawPlanItem>,
    #[serde(default)]
    initial_required_checks: LegacyField<Vec<RequiredCheck>>,
}

impl<'de> Deserialize<'de> for PlanSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawPlanSnapshot::deserialize(deserializer)?;
        let plan = match raw.format_version {
            LegacyField::Missing => {
                if !raw.summary.is_missing()
                    || !raw.initial_required_checks.is_missing()
                    || raw.items.iter().any(RawPlanItem::has_extensions)
                {
                    return Err(D::Error::custom(DomainError::InvalidPlan));
                }
                PlanSnapshot::legacy(
                    raw.revision,
                    raw.items.into_iter().map(RawPlanItem::legacy).collect(),
                )
            }
            LegacyField::Present(format_version) => PlanSnapshot {
                format_version,
                revision: raw.revision,
                summary: raw.summary.into_present().map_err(D::Error::custom)?,
                items: raw
                    .items
                    .into_iter()
                    .map(RawPlanItem::explicit)
                    .collect::<Result<_, _>>()
                    .map_err(D::Error::custom)?,
                initial_required_checks: raw
                    .initial_required_checks
                    .into_present()
                    .map_err(D::Error::custom)?,
            },
        };
        plan.validate().map_err(D::Error::custom)?;
        Ok(plan)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PlanItem {
    id: String,
    title: String,
    description: String,
    acceptance_criteria: Vec<String>,
    status: PlanItemStatus,
}

impl PlanItem {
    pub fn legacy(id: impl Into<String>, title: impl Into<String>, status: PlanItemStatus) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            description: String::new(),
            acceptance_criteria: Vec::new(),
            status,
        }
    }

    pub fn try_structured(
        id: impl Into<String>,
        title: impl Into<String>,
        description: impl Into<String>,
        acceptance_criteria: Vec<String>,
        status: PlanItemStatus,
    ) -> Result<Self, DomainError> {
        let item = Self {
            id: id.into(),
            title: title.into(),
            description: description.into(),
            acceptance_criteria,
            status,
        };
        item.validate_structured()?;
        Ok(item)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn acceptance_criteria(&self) -> &[String] {
        &self.acceptance_criteria
    }

    pub const fn status(&self) -> PlanItemStatus {
        self.status
    }

    pub fn into_parts(self) -> (String, String, String, Vec<String>, PlanItemStatus) {
        (
            self.id,
            self.title,
            self.description,
            self.acceptance_criteria,
            self.status,
        )
    }

    fn into_legacy(self) -> Self {
        Self::legacy(self.id, self.title, self.status)
    }

    fn validate_structured(&self) -> Result<(), DomainError> {
        if self.id.is_empty()
            || self.title.is_empty()
            || self.title.chars().count() > MAX_PLAN_TITLE_SCALARS
            || self.description.chars().count() > MAX_PLAN_DESCRIPTION_SCALARS
            || !(1..=MAX_ACCEPTANCE_CRITERIA).contains(&self.acceptance_criteria.len())
            || self.acceptance_criteria.iter().any(|criterion| {
                criterion.is_empty() || criterion.chars().count() > MAX_ACCEPTANCE_CRITERION_SCALARS
            })
        {
            return Err(DomainError::InvalidPlan);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPlanItem {
    id: String,
    title: String,
    #[serde(default)]
    description: LegacyField<String>,
    #[serde(default)]
    acceptance_criteria: LegacyField<Vec<String>>,
    status: PlanItemStatus,
}

impl RawPlanItem {
    fn has_extensions(&self) -> bool {
        !self.description.is_missing() || !self.acceptance_criteria.is_missing()
    }

    fn legacy(self) -> PlanItem {
        PlanItem::legacy(self.id, self.title, self.status)
    }

    fn explicit(self) -> Result<PlanItem, DomainError> {
        Ok(PlanItem {
            id: self.id,
            title: self.title,
            description: self.description.into_present()?,
            acceptance_criteria: self.acceptance_criteria.into_present()?,
            status: self.status,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanItemStatus {
    Pending,
    Running,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ActivityEntry {
    id: String,
    level: ActivityLevel,
    actor: ActivityActor,
    role_run: Option<u32>,
    message: String,
    created_at: UtcTimestamp,
}

impl ActivityEntry {
    pub fn legacy(
        id: impl Into<String>,
        level: ActivityLevel,
        message: impl Into<String>,
        created_at: UtcTimestamp,
    ) -> Self {
        Self {
            id: id.into(),
            level,
            actor: ActivityActor::System,
            role_run: None,
            message: message.into(),
            created_at,
        }
    }

    pub fn try_new(
        id: impl Into<String>,
        level: ActivityLevel,
        actor: ActivityActor,
        role_run: Option<u32>,
        message: impl Into<String>,
        created_at: UtcTimestamp,
    ) -> Result<Self, DomainError> {
        if match actor {
            ActivityActor::System => role_run.is_some(),
            ActivityActor::Planner | ActivityActor::Executor | ActivityActor::Reviewer => {
                !role_run.is_some_and(|value| value > 0)
            }
        } {
            return Err(DomainError::InvalidActivity);
        }
        Ok(Self {
            id: id.into(),
            level,
            actor,
            role_run,
            message: message.into(),
            created_at,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub const fn level(&self) -> ActivityLevel {
        self.level
    }

    pub const fn actor(&self) -> ActivityActor {
        self.actor
    }

    pub const fn role_run(&self) -> Option<u32> {
        self.role_run
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn created_at(&self) -> UtcTimestamp {
        self.created_at
    }

    pub fn into_parts(
        self,
    ) -> (
        String,
        ActivityLevel,
        ActivityActor,
        Option<u32>,
        String,
        UtcTimestamp,
    ) {
        (
            self.id,
            self.level,
            self.actor,
            self.role_run,
            self.message,
            self.created_at,
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawActivityEntry {
    id: String,
    level: ActivityLevel,
    #[serde(default)]
    actor: LegacyField<ActivityActor>,
    #[serde(default)]
    role_run: LegacyField<Option<u32>>,
    message: String,
    created_at: UtcTimestamp,
}

impl<'de> Deserialize<'de> for ActivityEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawActivityEntry::deserialize(deserializer)?;
        match (raw.actor, raw.role_run) {
            (LegacyField::Missing, LegacyField::Missing) => {
                Ok(Self::legacy(raw.id, raw.level, raw.message, raw.created_at))
            }
            (LegacyField::Present(actor), LegacyField::Present(role_run)) => Self::try_new(
                raw.id,
                raw.level,
                actor,
                role_run,
                raw.message,
                raw.created_at,
            )
            .map_err(D::Error::custom),
            _ => Err(D::Error::custom(DomainError::InvalidActivity)),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityActor {
    #[default]
    System,
    Planner,
    Executor,
    Reviewer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DiffSnapshot {
    pub revision: u64,
    pub files: Vec<DiffFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DiffFile {
    pub path: String,
    pub status: DiffFileStatus,
    pub patch: String,
    pub additions: u64,
    pub deletions: u64,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffFileStatus {
    Added,
    Modified,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TestSnapshot {
    pub revision: u64,
    pub status: TestStatus,
    pub cases: Vec<TestCase>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TestCase {
    pub id: String,
    pub name: String,
    pub status: TestStatus,
    pub duration_ms: u64,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestStatus {
    Queued,
    Running,
    Passed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TaskEventKind {
    #[serde(rename = "task.queued")]
    TaskQueued,
    #[serde(rename = "task.started")]
    TaskStarted,
    #[serde(rename = "plan.updated")]
    PlanUpdated,
    #[serde(rename = "activity.appended")]
    ActivityAppended,
    #[serde(rename = "diff.updated")]
    DiffUpdated,
    #[serde(rename = "test.updated")]
    TestUpdated,
    #[serde(rename = "review.updated")]
    ReviewUpdated,
    #[serde(rename = "task.completed")]
    TaskCompleted,
    #[serde(rename = "task.failed")]
    TaskFailed,
    #[serde(rename = "task.cancelled")]
    TaskCancelled,
    #[serde(rename = "task.interrupted")]
    TaskInterrupted,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "payload")]
pub enum TaskEventPayload {
    #[serde(rename = "task.queued")]
    TaskQueued { task: Task },
    #[serde(rename = "task.started")]
    TaskStarted { task: Task },
    #[serde(rename = "plan.updated")]
    PlanUpdated { plan: PlanSnapshot },
    #[serde(rename = "activity.appended")]
    ActivityAppended { entry: ActivityEntry },
    #[serde(rename = "diff.updated")]
    DiffUpdated { diff: DiffSnapshot },
    #[serde(rename = "test.updated")]
    TestUpdated { tests: TestSnapshot },
    #[serde(rename = "review.updated")]
    ReviewUpdated { review: ReviewEvidence },
    #[serde(rename = "task.completed")]
    TaskCompleted { task: Task },
    #[serde(rename = "task.failed")]
    TaskFailed { task: Task },
    #[serde(rename = "task.cancelled")]
    TaskCancelled { task: Task },
    #[serde(rename = "task.interrupted")]
    TaskInterrupted { task: Task },
}

impl TaskEventPayload {
    pub const fn kind(&self) -> TaskEventKind {
        match self {
            Self::TaskQueued { .. } => TaskEventKind::TaskQueued,
            Self::TaskStarted { .. } => TaskEventKind::TaskStarted,
            Self::PlanUpdated { .. } => TaskEventKind::PlanUpdated,
            Self::ActivityAppended { .. } => TaskEventKind::ActivityAppended,
            Self::DiffUpdated { .. } => TaskEventKind::DiffUpdated,
            Self::TestUpdated { .. } => TaskEventKind::TestUpdated,
            Self::ReviewUpdated { .. } => TaskEventKind::ReviewUpdated,
            Self::TaskCompleted { .. } => TaskEventKind::TaskCompleted,
            Self::TaskFailed { .. } => TaskEventKind::TaskFailed,
            Self::TaskCancelled { .. } => TaskEventKind::TaskCancelled,
            Self::TaskInterrupted { .. } => TaskEventKind::TaskInterrupted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TaskEvent {
    pub id: EventId,
    pub schema_version: u16,
    pub task_id: TaskId,
    #[serde(flatten)]
    pub payload: TaskEventPayload,
    pub created_at: UtcTimestamp,
}

impl TaskEvent {
    pub fn new(
        id: EventId,
        task_id: TaskId,
        payload: TaskEventPayload,
        created_at: UtcTimestamp,
    ) -> Self {
        Self {
            id,
            schema_version: 1,
            task_id,
            payload,
            created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TimelineEntry {
    pub event_id: EventId,
    pub kind: TaskEventKind,
    pub label: String,
    pub created_at: UtcTimestamp,
    pub failure: Option<TaskFailure>,
}
