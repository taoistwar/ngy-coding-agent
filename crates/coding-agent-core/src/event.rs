#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentEvent {
    Plan(PlanEvent),
    Activity(ActivityEvent),
    Diff(DiffEvent),
    Tests(TestEvent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanEvent {
    pub revision: u64,
    pub items: Vec<PlanItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanItem {
    pub id: String,
    pub title: String,
    pub status: PlanItemStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanItemStatus {
    Pending,
    Running,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityEvent {
    pub level: ActivityLevel,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffEvent {
    pub revision: u64,
    pub files: Vec<DiffFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffFile {
    pub path: String,
    pub status: DiffFileStatus,
    pub patch: String,
    pub additions: u64,
    pub deletions: u64,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffFileStatus {
    Added,
    Modified,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestEvent {
    pub revision: u64,
    pub status: TestStatus,
    pub cases: Vec<TestCase>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestCase {
    pub id: String,
    pub name: String,
    pub status: TestStatus,
    pub duration_ms: Option<u64>,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestStatus {
    Queued,
    Running,
    Passed,
    Failed,
    Cancelled,
}
