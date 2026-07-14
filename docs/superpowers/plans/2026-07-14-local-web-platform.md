# Local Web Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Project 1 as a directly launchable local Rust application with a protected React three-pane UI, deterministic fake tasks, SQLite persistence, REST commands, replayable SSE, restart recovery, and cross-platform smoke coverage.

**Architecture:** Four Rust crates preserve the approved dependency direction: `domain` owns pure rules, `store` owns SQLite, `api` owns REST/SSE contracts, and `app` composes actors and native platform services. React consumes OpenAPI-generated TypeScript types, treats REST snapshots as authoritative, and applies persisted SSE events as idempotent deltas. Project 1 uses only `FakeTaskRunner`; real model, code tools, worktrees, roles, and merge behavior remain outside this plan.

**Tech Stack:** Rust 1.97.0, edition 2024, Axum 0.8.9, Tokio 1.52.3, SQLx 0.9.0 with SQLite, Utoipa 5.5.0, React 19.2.7, TypeScript 5.9.3, Vite 8.1.4, Vitest 4.1.10, Playwright 1.61.1.

## Global Constraints

- Execute this plan in an isolated worktree created with `superpowers:using-git-worktrees`; do not implement directly on `main`.
- Project 1 may inspect only Git/Cargo registration metadata. It must not read source files, call a model, create a worktree, modify a repository, or run repository tests.
- The production fake runner concurrency is exactly 4. The future real runner defaults to 1 until Project 4; do not encode fake concurrency into API types.
- `TaskStatus::Completed` means runner success only. It never means reviewed, deliverable, or mergeable.
- SQLite is authoritative. Every observable task state change and its event commit in one transaction; memory channels are acceleration only.
- Task claim/cancel, runner event/result, reconciliation, and quiesce are serialized by `TaskManager`; after runtime actors start, create/retry and every SQLite mutation are serialized by `StoreWriter`; migration/startup recovery run earlier under sole primary ownership. Every persisted live event is published in ID order by `EventDispatcher`.
- Bind only `127.0.0.1` on a random port. Production has no CORS and no remote/CDN assets.
- All `/api/*` reads require the process session. Every mutation additionally requires exact Origin and `X-CSRF-Token`.
- Browser refresh or close never cancels work. Startup converts persisted `Queued`/`Running` tasks to `Interrupted`; it never auto-resumes them.
- Rust OpenAPI is the only API DTO source. Frontend aliases may reference generated types but may not duplicate DTO shapes.
- All persisted and API timestamps pass through `UtcTimestamp` and serialize as UTC RFC 3339; repository paths pass through `CanonicalPath` before persistence or response mapping.
- Release order is OpenAPI export → `npm ci` → TypeScript generation check → typecheck/tests/build → Rust checks/tests → release build.
- Dependency installation may use registries; test execution and normal application runtime must make no external network request.
- Use TDD for every behavior: observe the focused test fail for the intended reason, implement the minimum behavior, observe it pass, run impacted suites, then commit.
- Do not add compatibility layers for Project 2. Only keep the approved `TaskRunner` and versioned event seams.

## Source Specifications

- `docs/superpowers/specs/2026-07-14-coding-agent-product-roadmap-design.md`
- `docs/superpowers/specs/2026-07-14-local-web-platform-design.md`

## Locked File Map

```text
Cargo.toml                                  Rust workspace and locked shared dependencies
Cargo.lock                                  committed Rust dependency lock
rust-toolchain.toml                         Rust 1.97.0 toolchain pin
crates/coding-agent-domain/
  Cargo.toml
  src/lib.rs                                public domain exports
  src/ids.rs                                UUID newtypes
  src/value.rs                              validated path/time/event cursor values
  src/repository.rs                         Repository and registration inputs
  src/task.rs                               Task state machine and failure
  src/event.rs                              event kinds and panel snapshots
  tests/state_machine.rs                    legal transition and retry tests
crates/coding-agent-store/
  Cargo.toml
  migrations/0001_initial.sql               repositories/tasks/events/schema_migrations
  src/lib.rs                                Store entrypoint and read pool
  src/migrate.rs                            embedded monotonic migration runner
  src/repositories.rs                       repository identity and upsert
  src/tasks.rs                              task/event transactions and recovery
  src/projection.rs                         BootstrapSnapshot and TaskDetail replay
  tests/migrations.rs
  tests/repositories.rs
  tests/tasks.rs
  tests/projection.rs
  tests/support/mod.rs                    shared SQLite fixtures imported by each test target
crates/coding-agent-api/
  Cargo.toml
  src/lib.rs
  src/contract.rs                           REST/SSE/OpenAPI DTOs
  src/backend.rs                            ApiBackend and RequestSecurity ports
  src/error.rs                              ApiErrorResponse mapping
  src/router.rs                             protected REST route handlers
  src/sse.rs                                SSE join and wire frames
  src/bin/export_openapi.rs                 deterministic OpenAPI exporter
  tests/openapi.rs
  tests/router.rs
  tests/sse.rs
  tests/support/mod.rs                    fake API/security/SSE ports
crates/coding-agent-app/
  Cargo.toml
  build.rs                                  frontend embed rebuild trigger
  src/lib.rs
  src/main.rs                               primary/secondary composition root
  src/service_state.rs                      ready/degraded/quiescing generation
  src/store_writer.rs                       single SQLite mutation actor
  src/event_dispatcher.rs                   ordered DB-backed live publisher
  src/task_manager.rs                       single task-control actor
  src/fake_runner.rs                        deterministic runner and test scripts
  src/repository_service.rs                 read-only Git/Cargo discovery
  src/native_dialog.rs                      serialized picker/message dialog port
  src/security.rs                           session, token, Host, Origin, CSRF
  src/platform.rs                           app paths, private permissions, browser
  src/single_instance.rs                    file lock and runtime descriptor
  src/server.rs                             outer Axum router and readiness
  src/static_assets.rs                      dev fallback and release embedding
  src/shutdown.rs                           normal and degraded shutdown
  src/test_support.rs                       feature-gated process test injection
  tests/store_writer.rs
  tests/event_dispatcher.rs
  tests/task_manager.rs
  tests/degraded_recovery.rs
  tests/platform.rs
  tests/repository_service.rs
  tests/security.rs
  tests/single_instance.rs
  tests/server.rs
  tests/static_assets.rs
  tests/shutdown.rs
  tests/process_support.rs
  tests/release_smoke.rs
  tests/support/mod.rs                    shared actor/platform fixtures imported by each test target
web/
  package.json
  package-lock.json
  tsconfig.json
  tsconfig.app.json
  tsconfig.node.json
  vite.config.ts
  vitest.config.ts
  playwright.config.ts
  index.html
  openapi.json
  scripts/generate-api.mjs
  src/api/generated/schema.d.ts
  src/api/types.ts
  src/api/client.ts
  src/api/sse.ts
  src/state/model.ts
  src/state/reducer.ts
  src/state/useAgentState.ts
  src/components/AppShell.tsx
  src/components/Sidebar.tsx
  src/components/TaskWorkspace.tsx
  src/components/TaskComposer.tsx
  src/components/PlanPane.tsx
  src/components/ActivityPane.tsx
  src/components/ResultPane.tsx
  src/components/ConnectionBanner.tsx
  src/components/ErrorBoundary.tsx
  src/styles.css
  src/main.tsx
  src/vite-env.d.ts
  src/test/setup.ts
  src/**/*.test.ts(x)
  e2e/local-app.spec.ts
  e2e/support/localApp.ts
.github/workflows/ci.yml                  Rust, frontend, E2E, three-OS smoke gates
scripts/check-placeholders.mjs             tracked-source forbidden-marker gate
README.md                                 direct-launch and development workflow
```

---

### Task 1: Establish the Workspace and Pure Domain Model

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `crates/coding-agent-domain/Cargo.toml`
- Create: `crates/coding-agent-domain/src/lib.rs`
- Create: `crates/coding-agent-domain/src/ids.rs`
- Create: `crates/coding-agent-domain/src/value.rs`
- Create: `crates/coding-agent-domain/src/repository.rs`
- Create: `crates/coding-agent-domain/src/task.rs`
- Create: `crates/coding-agent-domain/src/event.rs`
- Create: `crates/coding-agent-domain/tests/state_machine.rs`
- Create: `crates/coding-agent-store/Cargo.toml`
- Create: `crates/coding-agent-store/src/lib.rs`
- Create: `crates/coding-agent-api/Cargo.toml`
- Create: `crates/coding-agent-api/src/lib.rs`
- Create: `crates/coding-agent-app/Cargo.toml`
- Create: `crates/coding-agent-app/src/lib.rs`
- Modify: `.gitignore`

**Interfaces:**
- Produces: `RepositoryId`, `TaskId`, `ClientRequestId`, `CanonicalPath`, `UtcTimestamp`, `EventId`, `EventCursor`, `DomainError`, `Repository`, `NewRepository`, `Task`, `NewTask`, `TaskStatus`, `TaskFailure`, `TaskEvent`, `TaskEventKind`, `TaskEventPayload`, `PlanSnapshot`, `ActivityEntry`, `DiffSnapshot`, `TestSnapshot`, and `TimelineEntry`.
- Invariant: `TaskStatus::can_transition_to` is the only legal-transition table; store code may not duplicate it.

- [ ] **Step 1: Create the workspace manifests and a failing state-machine test**

Use this workspace dependency set and commit `Cargo.lock` when Cargo generates it; the independent npm lockfile is created in Task 16:

```toml
[workspace]
members = [
  "crates/coding-agent-domain",
  "crates/coding-agent-store",
  "crates/coding-agent-api",
  "crates/coding-agent-app",
]
resolver = "3"

[workspace.package]
edition = "2024"
rust-version = "1.97"
license = "MIT"

[workspace.dependencies]
async-stream = "0.3.6"
async-trait = "0.1.89"
axum = { version = "0.8.9", default-features = false, features = ["http1", "json", "macros", "matched-path", "original-uri", "query", "tokio"] }
axum-extra = { version = "0.12.6", default-features = false, features = ["cookie"] }
base64 = "0.22.1"
directories = "6.0.0"
futures-util = "0.3.32"
getrandom = "0.4.3"
http = "1.4.2"
http-body-util = "0.1.4"
mime_guess = "2.0.5"
rfd = "0.17.2"
rust-embed = "8.12.0"
serde = { version = "1.0.228", features = ["derive"] }
serde_json = "1.0.150"
sqlx = { version = "0.9.0", default-features = false, features = ["runtime-tokio", "sqlite-bundled", "json", "uuid", "time"] }
subtle = "2.6.1"
tempfile = "3.27.0"
thiserror = "2.0.18"
time = { version = "0.3.53", features = ["serde", "formatting", "parsing", "macros"] }
tokio = { version = "1.52.3", features = ["fs", "macros", "net", "process", "rt-multi-thread", "signal", "sync", "test-util", "time"] }
tokio-stream = { version = "0.1.18", features = ["sync"] }
tokio-util = { version = "0.7.18", features = ["rt"] }
tower = { version = "0.5.3", features = ["util"] }
tower-http = { version = "0.7.0", default-features = false, features = ["catch-panic", "request-id", "set-header", "trace"] }
tracing = "0.1.44"
tracing-subscriber = { version = "0.3.23", features = ["env-filter", "fmt"] }
utoipa = { version = "5.5.0", features = ["axum_extras", "time", "uuid"] }
utoipa-axum = "0.2.0"
uuid = { version = "1.23.5", features = ["serde", "v4"] }
webbrowser = { version = "1.2.1", features = ["hardened"] }
windows-sys = { version = "0.61.2", features = ["Win32_Foundation", "Win32_Security", "Win32_Security_Authorization", "Win32_Storage_FileSystem", "Win32_System_Memory", "Win32_System_Threading"] }
```

```toml
# rust-toolchain.toml
[toolchain]
channel = "1.97.0"
profile = "minimal"
components = ["clippy", "rustfmt"]
```

Create all four member manifests now so Cargo reaches the intended domain compile failure. The three not-yet-implemented `src/lib.rs` files contain only a crate-level responsibility comment. Later tasks add runtime dependencies when code first uses them.

Add `/target`, `/web/node_modules`, `/web/dist`, and Playwright output directories to `.gitignore`; keep `Cargo.lock`, `web/package-lock.json`, `web/openapi.json`, and generated TypeScript declarations tracked.

```toml
# crates/coding-agent-domain/Cargo.toml
[package]
name = "coding-agent-domain"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
serde.workspace = true
thiserror.workspace = true
time.workspace = true
uuid.workspace = true
```

```toml
# crates/coding-agent-store/Cargo.toml
[package]
name = "coding-agent-store"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
coding-agent-domain = { path = "../coding-agent-domain" }
```

```toml
# crates/coding-agent-api/Cargo.toml
[package]
name = "coding-agent-api"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
coding-agent-domain = { path = "../coding-agent-domain" }
```

```toml
# crates/coding-agent-app/Cargo.toml
[package]
name = "coding-agent-app"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
coding-agent-api = { path = "../coding-agent-api" }
coding-agent-domain = { path = "../coding-agent-domain" }
coding-agent-store = { path = "../coding-agent-store" }
```

```rust
// crates/coding-agent-domain/tests/state_machine.rs
use coding_agent_domain::TaskStatus;

#[test]
fn task_status_transition_matrix_is_closed() {
    use TaskStatus::*;
    let legal = [
        (Queued, Running),
        (Queued, Cancelled),
        (Queued, Interrupted),
        (Running, Completed),
        (Running, Failed),
        (Running, Cancelled),
        (Running, Interrupted),
    ];
    for from in [Queued, Running, Completed, Failed, Cancelled, Interrupted] {
        for to in [Queued, Running, Completed, Failed, Cancelled, Interrupted] {
            assert_eq!(from.can_transition_to(to), legal.contains(&(from, to)));
        }
    }
}

#[test]
fn only_terminal_tasks_are_retryable() {
    use TaskStatus::*;
    assert!(!Queued.is_retryable());
    assert!(!Running.is_retryable());
    for status in [Completed, Failed, Cancelled, Interrupted] {
        assert!(status.is_retryable());
    }
}
```

- [ ] **Step 2: Run the focused test and confirm the intended failure**

Run: `cargo test -p coding-agent-domain --test state_machine`

Expected: compilation fails because `coding_agent_domain::TaskStatus` does not yet exist. A toolchain or dependency-download failure is not the expected red result; fix the environment and rerun until the missing type is the failure.

- [ ] **Step 3: Implement the domain types and the single transition table**

Use transparent UUID newtypes and keep HTTP/OpenAPI names out of this crate:

```rust
// crates/coding-agent-domain/src/task.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl TaskStatus {
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Queued, Self::Running)
                | (Self::Queued, Self::Cancelled)
                | (Self::Queued, Self::Interrupted)
                | (Self::Running, Self::Completed)
                | (Self::Running, Self::Failed)
                | (Self::Running, Self::Cancelled)
                | (Self::Running, Self::Interrupted)
        )
    }

    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TaskFailure {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}
```

The remaining domain shapes are fixed here and reused unchanged by store/API mapping:

```rust
pub struct NewRepository {
    pub selected_path: CanonicalPath,
    pub display_name: String,
    pub git_root: CanonicalPath,
    pub cargo_workspace_root: CanonicalPath,
}

pub struct Repository {
    pub id: RepositoryId,
    pub selected_path: CanonicalPath,
    pub display_name: String,
    pub git_root: CanonicalPath,
    pub cargo_workspace_root: CanonicalPath,
    pub created_at: UtcTimestamp,
    pub last_opened_at: UtcTimestamp,
}

pub struct NewTask {
    pub client_request_id: ClientRequestId,
    pub repository_id: RepositoryId,
    pub prompt: String,
}

pub struct Task {
    pub id: TaskId,
    pub client_request_id: ClientRequestId,
    pub repository_id: RepositoryId,
    pub prompt: String,
    pub status: TaskStatus,
    pub attempt: u32,
    pub retry_of: Option<TaskId>,
    pub created_at: UtcTimestamp,
    pub started_at: Option<UtcTimestamp>,
    pub finished_at: Option<UtcTimestamp>,
    pub last_event_id: EventId,
    pub failure: Option<TaskFailure>,
}

pub struct PlanSnapshot { pub revision: u64, pub items: Vec<PlanItem> }
pub struct PlanItem { pub id: String, pub title: String, pub status: PlanItemStatus }
pub enum PlanItemStatus { Pending, Running, Completed }

pub struct ActivityEntry {
    pub id: String,
    pub level: ActivityLevel,
    pub message: String,
    pub created_at: UtcTimestamp,
}
pub enum ActivityLevel { Info, Warning, Error }

pub struct DiffSnapshot { pub revision: u64, pub files: Vec<DiffFile> }
pub struct DiffFile {
    pub path: String,
    pub status: DiffFileStatus,
    pub patch: String,
    pub additions: u64,
    pub deletions: u64,
}
pub enum DiffFileStatus { Added, Modified, Deleted }

pub struct TestSnapshot {
    pub revision: u64,
    pub status: TestStatus,
    pub cases: Vec<TestCase>,
}
pub struct TestCase {
    pub id: String,
    pub name: String,
    pub status: TestStatus,
    pub duration_ms: u64,
    pub summary: String,
}
pub enum TestStatus { Queued, Running, Passed, Failed, Cancelled }

pub struct TimelineEntry {
    pub event_id: EventId,
    pub kind: TaskEventKind,
    pub label: String,
    pub created_at: UtcTimestamp,
    pub failure: Option<TaskFailure>,
}
```

All structs/enums derive the appropriate Debug/Clone/Eq/serde traits; status enums use `snake_case`. `RepositoryId`, `TaskId`, and `ClientRequestId` are distinct transparent `uuid::Uuid` newtypes with `new()` and `Display`/`FromStr`. `NewTask::try_new` trims the prompt, rejects empty or more than 50,000 Unicode scalar values through `DomainError::InvalidPrompt`, and stores the trimmed value. Store constructors enforce `attempt >= 1` and `last_event_id > 0`. Queued has no timestamps/failure; Running has start only; Completed has start/finish and no failure; Failed has start/finish plus failure; Cancelled and Interrupted have finish with optional start, and only Interrupted requires failure.

Define IDs in `ids.rs`, repository types in `repository.rs`, and these exact event variants in `event.rs`:

```rust
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
    #[serde(rename = "task.completed")]
    TaskCompleted { task: Task },
    #[serde(rename = "task.failed")]
    TaskFailed { task: Task },
    #[serde(rename = "task.cancelled")]
    TaskCancelled { task: Task },
    #[serde(rename = "task.interrupted")]
    TaskInterrupted { task: Task },
}

pub struct TaskEvent {
    pub id: EventId,
    pub schema_version: u16,
    pub task_id: TaskId,
    #[serde(flatten)]
    pub payload: TaskEventPayload,
    pub created_at: UtcTimestamp,
}
```

`CanonicalPath::try_from_canonical` accepts only absolute, normalized paths without current/parent components; platform discovery performs filesystem canonicalization before calling it. `UtcTimestamp` normalizes to `UtcOffset::UTC`, parses RFC 3339, and always serializes the fixed-width UTC form `YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ` so SQLite text order equals chronological order. It never exposes a non-UTC value. `EventId::new` accepts only positive values; `EventCursor::new` accepts nonnegative values and provides `ZERO`. All four are private-field serde newtypes with checked constructors/accessors; event ID/cursor values derive Copy/Ord and convert explicitly at SQL/API boundaries.

`TaskEventKind` exhaustively mirrors those ten variants and `TaskEventPayload::kind()` is the only mapping. `TaskEvent::new` fixes schema version to 1. Extend `state_machine.rs` with prompt tests at empty, 50,000, and 50,001 scalars; UUID and event ID/cursor round trips; canonical-path rejection; non-UTC timestamp normalization/RFC3339 output; Task invariants; and tagged event serialization. No type contains a review/delivery field.

- [ ] **Step 4: Run domain tests and confirm green**

Run: `cargo test -p coding-agent-domain`

Expected: state-machine, prompt boundary, ID, invariant, and tagged-event tests pass with zero failures.

- [ ] **Step 5: Run formatting and lint for the new crate**

Run: `cargo fmt --all --check`

Run: `cargo clippy -p coding-agent-domain --all-targets -- -D warnings`

Expected: both commands exit 0 with no diagnostics.

- [ ] **Step 6: Commit the independently testable domain foundation**

```bash
git add Cargo.toml Cargo.lock rust-toolchain.toml .gitignore crates/coding-agent-domain crates/coding-agent-store/Cargo.toml crates/coding-agent-store/src/lib.rs crates/coding-agent-api/Cargo.toml crates/coding-agent-api/src/lib.rs crates/coding-agent-app/Cargo.toml crates/coding-agent-app/src/lib.rs
git commit -m "feat: add project domain model"
```

### Task 2: Add SQLite Migrations and Repository Registration

**Files:**
- Modify: `crates/coding-agent-store/Cargo.toml`
- Create: `crates/coding-agent-store/migrations/0001_initial.sql`
- Modify: `crates/coding-agent-store/src/lib.rs`
- Create: `crates/coding-agent-store/src/migrate.rs`
- Create: `crates/coding-agent-store/src/repositories.rs`
- Create: `crates/coding-agent-store/tests/migrations.rs`
- Create: `crates/coding-agent-store/tests/repositories.rs`
- Create: `crates/coding-agent-store/tests/support/mod.rs`

**Interfaces:**
- Consumes: domain `Repository`, `RepositoryId`, and `NewRepository`.
- Produces: `Store::open`, `Store::migrate`, `Store::register_repository`, `RegisterRepositoryOutcome::{Created, Existing}`, and read-only `Store::list_repositories` ordered by `(last_opened_at DESC,id)` without pagination.
- Invariant: the display path never defines identity; `(git_identity_key, cargo_identity_key)` is unique.

The store manifest delta for this task is exact:

```toml
[dependencies]
coding-agent-domain = { path = "../coding-agent-domain" }
serde_json.workspace = true
sqlx.workspace = true
thiserror.workspace = true
time.workspace = true
uuid.workspace = true

[dev-dependencies]
tempfile.workspace = true
tokio.workspace = true
```

- [ ] **Step 1: Write failing migration and repository idempotency tests**

```rust
// crates/coding-agent-store/tests/repositories.rs
#[tokio::test]
async fn registering_the_same_workspace_reuses_the_row() {
    let fixture = support::store_fixture().await;
    let input = fixture.canonical_repository_input("repo").await;
    let first = fixture.store.register_repository(input.clone()).await.unwrap();
    let second = fixture.store.register_repository(input).await.unwrap();
    assert!(matches!(first, RegisterRepositoryOutcome::Created(_)));
    assert!(matches!(second, RegisterRepositoryOutcome::Existing(_)));
    assert_eq!(fixture.store.list_repositories().await.unwrap().len(), 1);
}
```

`tests/support/mod.rs` owns `memory_store`, file-backed temporary Store, repository builders, and database fault helpers; every store integration test begins with `mod support;`. `migrations.rs` must assert `PRAGMA journal_mode`, `PRAGMA foreign_keys`, a non-zero busy timeout, idempotent second migration, and existence of `schema_migrations`, `repositories`, `tasks`, and `task_events`.

- [ ] **Step 2: Run store tests and verify the red result**

Run: `cargo test -p coding-agent-store --test migrations --test repositories`

Expected: compilation fails because `Store` and the migration do not exist.

- [ ] **Step 3: Implement the schema and embedded monotonic migration runner**

The initial SQL must contain these constraints, with timestamps stored as RFC 3339 text and UUIDs as lowercase text:

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE repositories (
    id TEXT PRIMARY KEY,
    selected_path TEXT NOT NULL,
    display_name TEXT NOT NULL,
    git_root TEXT NOT NULL,
    cargo_workspace_root TEXT NOT NULL,
    git_identity_key TEXT NOT NULL,
    cargo_identity_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_opened_at TEXT NOT NULL,
    UNIQUE (git_identity_key, cargo_identity_key)
);

CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    client_request_id TEXT NOT NULL UNIQUE,
    repository_id TEXT NOT NULL REFERENCES repositories(id),
    prompt TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued','running','completed','failed','cancelled','interrupted')),
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    retry_of TEXT UNIQUE REFERENCES tasks(id),
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    last_event_id INTEGER NOT NULL DEFAULT 0 CHECK (last_event_id >= 0),
    failure_json TEXT
);

CREATE TABLE task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_version INTEGER NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    created_at TEXT NOT NULL
);

CREATE INDEX task_events_task_id_id ON task_events(task_id, id);
```

`Store::open` uses `SqliteConnectOptions` with create-if-missing, WAL, foreign keys, and a five-second busy timeout. `migrate` runs `BEGIN IMMEDIATE`, applies the ordered table beginning with `(1, include_str!("../migrations/0001_initial.sql"))` when absent from `schema_migrations`, inserts the version, and commits. Never auto-delete or recreate a failing database.

- [ ] **Step 4: Implement repository identity upsert**

`register_repository` starts an immediate transaction, looks up the identity pair, updates `selected_path` and `last_opened_at` for an existing row, otherwise inserts a UUID v4 row. On Windows, canonical paths pass through one `windows_identity_key` function that normalizes separators and uses Unicode lowercase; tests must cover a case-variant input. Unix identity keys preserve case.

- [ ] **Step 5: Run focused and impacted tests**

Run: `cargo test -p coding-agent-store --test migrations --test repositories`

Run: `cargo test -p coding-agent-domain -p coding-agent-store`

Expected: all tests pass. The second migration call leaves one version row, and duplicate registration leaves one repository row.

- [ ] **Step 6: Commit the repository persistence slice**

```bash
git add crates/coding-agent-store Cargo.lock
git commit -m "feat: persist registered repositories"
```

### Task 3: Add Atomic Task/Event Transactions and Projections

**Files:**
- Create: `crates/coding-agent-store/src/tasks.rs`
- Create: `crates/coding-agent-store/src/projection.rs`
- Create: `crates/coding-agent-store/tests/tasks.rs`
- Create: `crates/coding-agent-store/tests/projection.rs`
- Modify: `crates/coding-agent-store/tests/support/mod.rs`
- Modify: `crates/coding-agent-store/src/lib.rs`

**Interfaces:**
- Produces: `TaskTransition`, `CreateTaskOutcome`, `RetryTaskOutcome`, `TransitionOutcome`, `AppendEventOutcome`, `RecoveryOutcome`, `BootstrapSnapshot`, `TaskDetail`, `EventPage`, and store methods `create_task`, `retry_task`, `transition_with_event`, `append_running_event`, `recover_incomplete`, `bootstrap_snapshot`, `task_detail`, `events_after`, `task_events_after`, and `latest_event_id`.
- Invariant: each method that changes visible task state also inserts its event and updates `last_event_id` in the same transaction.

- [ ] **Step 1: Write failing transaction, retry, recovery, and projection tests**

```rust
#[tokio::test]
async fn transition_and_event_are_one_transaction() {
    let store = support::seeded_store().await;
    let task = support::queued_task(&store).await;
    let changed = store
        .transition_with_event(
            task.id,
            TaskStatus::Queued,
            TaskTransition::Running,
        )
        .await
        .unwrap();
    assert!(matches!(changed, TransitionOutcome::Applied { .. }));
    let detail = store.task_detail(task.id).await.unwrap().unwrap();
    assert_eq!(detail.task.status, TaskStatus::Running);
    assert_eq!(detail.timeline.last().unwrap().kind, TaskEventKind::TaskStarted);
    assert_eq!(detail.task.last_event_id, detail.timeline.last().unwrap().event_id);
}

#[tokio::test]
async fn retry_is_a_linear_idempotent_chain() {
    let store = support::seeded_store().await;
    let source = support::terminal_task(&store, TaskStatus::Interrupted).await;
    let a = store.retry_task(source.id).await.unwrap();
    let b = store.retry_task(source.id).await.unwrap();
    assert_eq!(a.task().id, b.task().id);
    assert_eq!(a.task().repository_id, source.repository_id);
    assert_eq!(a.task().prompt, source.prompt);
    assert_eq!(a.task().attempt, source.attempt + 1);
    assert_eq!(a.task().retry_of, Some(source.id));
    assert_ne!(a.task().client_request_id, source.client_request_id);
}
```

Add tests that initial create sets `attempt = 1` and `retry_of = None`, a mismatched repeated `client_request_id` returns `IDEMPOTENCY_CONFLICT`, an illegal transition changes neither task nor event count, startup recovery atomically interrupts all queued/running tasks, and `TaskDetail` reads events plus the global watermark from one SQLite read transaction.

Add a concurrent retry test that releases at least eight calls against one terminal source and observes one direct child ID/event. For every lifecycle event, assert `payload.task.last_event_id == event.id`. Inject a failure after task-state update, placeholder event insert, last-event update, and final payload update; every fault must roll the entire transaction back and leave no publishable event or placeholder payload.

- [ ] **Step 2: Run the focused tests and verify failure**

Run: `cargo test -p coding-agent-store --test tasks --test projection`

Expected: compilation fails on the missing task methods and projection types.

- [ ] **Step 3: Implement task creation, transitions, event append, and retry**

Use `BEGIN IMMEDIATE` for every mutation. Lifecycle payloads embed the final Task, so all create/retry/transition/recovery paths use one invisible transaction-local sequence: write the raw task row/state (creation may temporarily store integer zero without constructing a domain Task), insert the lifecycle event row with valid-JSON internal placeholder `{}`, obtain its AUTOINCREMENT EventId, update `tasks.last_event_id`, reload and validate the final domain Task, replace that event's placeholder with the typed payload containing the reloaded Task, verify exactly one payload row changed, and commit. No public/committed Task has ID zero, no committed event has a placeholder, and every lifecycle payload Task points back to its own event ID.

Repeated create request IDs compare repository and trimmed prompt. `transition_with_event(task_id, expected, transition)` validates the state table and conditionally updates by both id and expected status before the shared sequence. Non-lifecycle running events can insert their final typed payload immediately, then update Task.last_event_id before commit. Callers never supply lifecycle payloads or stale Task snapshots.

```rust
pub enum TaskTransition {
    Running,
    Completed,
    Failed(TaskFailure),
    Cancelled,
    Interrupted(TaskFailure),
}

impl TaskTransition {
    pub fn next(&self) -> TaskStatus {
        match self {
            Self::Running => TaskStatus::Running,
            Self::Completed => TaskStatus::Completed,
            Self::Failed(_) => TaskStatus::Failed,
            Self::Cancelled => TaskStatus::Cancelled,
            Self::Interrupted(_) => TaskStatus::Interrupted,
        }
    }

    pub fn failure(&self) -> Option<&TaskFailure> {
        match self {
            Self::Failed(failure) | Self::Interrupted(failure) => Some(failure),
            Self::Running | Self::Completed | Self::Cancelled => None,
        }
    }
}

pub enum CreateTaskOutcome {
    Created { task: Task, event_id: EventId },
    Existing { task: Task },
}

pub enum RetryTaskOutcome {
    Created { task: Task, event_id: EventId },
    Existing { task: Task },
}

pub enum TransitionOutcome {
    Applied { task: Task, event_id: EventId },
    Conflict { current: Task },
}

pub enum AppendEventOutcome {
    Applied { event_id: EventId },
    NotRunning { current: Task },
}

pub struct RecoveryOutcome {
    pub interrupted_count: usize,
    pub first_event_id: Option<EventId>,
    pub last_event_id: Option<EventId>,
    pub high_watermark: EventCursor,
}

pub struct EventPage {
    pub events: Vec<TaskEvent>,
    pub high_watermark: EventCursor,
}
```

The closed enum makes invalid failure combinations unrepresentable. `Running` sets `started_at = now`, clears finish/failure, and is valid only from Queued. Every terminal transition sets `finished_at = now` and preserves the existing `started_at`; `Failed` and `Interrupted` store their structured failure, while Completed/Cancelled clear it. Reject a table-invalid edge as `StoreError::IllegalTransition { from, to }` before SQL; a CAS miss returns `Conflict { current }`; a missing ID is `StoreError::TaskNotFound`. Create mismatch is `StoreError::IdempotencyConflict`; retry of a nonterminal task is `StoreError::TaskNotRetryable`.

`append_running_event` accepts only PlanUpdated, ActivityAppended, DiffUpdated, or TestUpdated payloads and returns `StoreError::InvalidRunningEvent` for lifecycle variants; it returns NotRunning unless the task is still Running. Initial create always sets `attempt = 1` and `retry_of = None`. `retry_task` accepts only terminal tasks, returns an existing direct child before inserting, and creates exactly one new queued child plus event. The child copies the source `repository_id` and prompt, sets `attempt = source.attempt + 1` and `retry_of = Some(source.id)`, and receives a fresh server-generated `ClientRequestId`; it never reuses the source request ID. Both create/retry outcome enums implement `task(&self) -> &Task`.

`recover_incomplete(now, failure)` updates all Queued/Running tasks and inserts one `task.interrupted` event per task in deterministic `(created_at,id)` order in a single transaction, then returns the database high watermark even when count is zero. Callers use failure codes `APP_RESTARTED`, `STORE_DEGRADED_RECOVERY`, or `APP_SHUTDOWN`, each with a stable user-safe message and `retryable = true`.

- [ ] **Step 4: Implement coherent bootstrap and TaskDetail replay**

`bootstrap_snapshot` reads all repositories ordered by `(last_opened_at DESC,id)`, all task summaries ordered by `(created_at DESC,id)`, and `MAX(task_events.id)` in one read transaction; Project 1 does not paginate or prune either list. `task_detail` starts one read transaction, loads all task events in ID order, projects the panel state, reads the same snapshot's global maximum into `event_cursor`, and commits.

```rust
pub struct TaskDetail {
    pub task: Task,
    pub plan: Option<PlanSnapshot>,
    pub activity: Vec<ActivityEntry>,
    pub diff: Option<DiffSnapshot>,
    pub tests: Option<TestSnapshot>,
    pub timeline: Vec<TimelineEntry>,
    pub event_cursor: EventCursor,
}
```

Projection rules replace plan/diff/tests snapshots, append activity by stable entry ID, and derive timeline only from task lifecycle variants.

- [ ] **Step 5: Run store suites and verify atomic behavior**

Run: `cargo test -p coding-agent-store`

Expected: all migration, repository, task, recovery, and projection tests pass with zero failures.

- [ ] **Step 6: Commit the authoritative task/event store**

```bash
git add crates/coding-agent-store
git commit -m "feat: add atomic task event store"
```

### Task 4: Define the API Contract and Deterministic OpenAPI Export

**Files:**
- Modify: `crates/coding-agent-api/Cargo.toml`
- Modify: `crates/coding-agent-api/src/lib.rs`
- Create: `crates/coding-agent-api/src/contract.rs`
- Create: `crates/coding-agent-api/src/backend.rs`
- Create: `crates/coding-agent-api/src/error.rs`
- Create: `crates/coding-agent-api/src/bin/export_openapi.rs`
- Create: `crates/coding-agent-api/tests/openapi.rs`

**Interfaces:**
- Consumes: domain models only; store projection/error mapping belongs to the app crate and this crate does not depend on `coding-agent-store` or `coding-agent-app`.
- Produces: `UtcTimestampDto`, `CanonicalPathDto`, all REST DTOs, the discriminator-based `TaskEventDto`, `StreamResetControl`, `ServiceStateControl`, `SseMessage`, `ApiError`, `ApiErrorResponse`, `CreateResult`, `CancelResult`, `QuitAcceptance`, `ApiBackend`, `SseBackend`, `RequestSecurity`, and `ApiDoc`.
- Invariant: Task event payloads are typed OpenAPI `oneOf`; only the explicitly open-ended API error `details` map may use `serde_json::Value`.

The API manifest delta is exact:

```toml
[dependencies]
async-trait.workspace = true
axum.workspace = true
coding-agent-domain = { path = "../coding-agent-domain" }
futures-util.workspace = true
http.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
time.workspace = true
utoipa.workspace = true
utoipa-axum.workspace = true
uuid.workspace = true

[dev-dependencies]
tempfile.workspace = true

[target.'cfg(windows)'.dependencies]
windows-sys.workspace = true
```

- [ ] **Step 1: Write failing OpenAPI contract tests**

```rust
#[test]
fn task_event_schema_is_a_discriminated_union() {
    let value = serde_json::to_value(ApiDoc::openapi()).unwrap();
    let schema = &value["components"]["schemas"]["TaskEventDto"];
    assert!(schema.get("oneOf").is_some());
    assert_eq!(schema["discriminator"]["propertyName"], "kind");
}

#[test]
fn openapi_contains_every_approved_component() {
    let value = serde_json::to_value(ApiDoc::openapi()).unwrap();
    for schema in [
        "TaskDto",
        "TaskDetailDto",
        "TaskEventDto",
        "BootstrapResponse",
        "StreamResetControl",
        "ServiceStateControl",
        "ApiErrorResponse",
    ] {
        assert!(value["components"]["schemas"].get(schema).is_some(), "missing {schema}");
    }
}
```

Add schema assertions that `TaskDto.last_event_id` is required and non-null; TaskDetail has nullable plan/diff/tests plus array activity/timeline; StreamResetControl is exactly schema version/kind/latest ID; and ServiceStateControl is exactly schema version/kind/state/generation. Add an exporter integration test that pre-creates the output with sentinel bytes, invokes `export_openapi` twice on that same path, and verifies each successful replacement is complete valid JSON with the canonical bytes. Endpoint path/response assertions begin in Task 12, when the real `utoipa-axum` router is the single path source.

- [ ] **Step 2: Run the contract test and verify the red result**

Run: `cargo test -p coding-agent-api --test openapi`

Expected: compilation fails because `ApiDoc` and contract DTOs do not exist.

- [ ] **Step 3: Implement exact transport DTOs and port traits**

Define private-field `UtcTimestampDto(String)` and `CanonicalPathDto(String)` transport scalars. Their only constructors consume domain UtcTimestamp/CanonicalPath; timestamp serialization is UTC RFC 3339 and its OpenAPI schema is string/date-time, while path is a platform string. DTO mapping never accepts arbitrary unvalidated strings for these fields.

Define `TaskEventDto` as a `#[serde(untagged)]` enum over ten concrete envelope structs. Every envelope has top-level `id`, `schema_version`, `task_id`, a single-value `kind` enum, its typed `payload`, and `created_at`; this preserves the approved flat wire frame instead of nesting envelope fields inside payload. Implement its Utoipa schema as `oneOf` those ten envelopes with `Discriminator::new("kind")`, and test both JSON shape and schema. Define `SseMessage` as `TaskEvent | StreamReset | ServiceState`, with control events carrying no persisted ID. `BootstrapResponse` includes `csrf_token`, repositories, tasks, `latest_event_id`, `server_started_at`, `service_state`, `service_state_generation`, and `max_concurrent_tasks`.

```rust
#[derive(serde::Serialize, utoipa::ToSchema)]
#[serde(untagged)]
pub enum SseMessage {
    TaskEvent(TaskEventDto),
    StreamReset(StreamResetControl),
    ServiceState(ServiceStateControl),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StreamResetControl {
    pub schema_version: u16,
    pub kind: StreamResetKind,
    pub latest_event_id: i64,
}
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub enum StreamResetKind { #[serde(rename = "stream.reset")] StreamReset }

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ServiceStateControl {
    pub schema_version: u16,
    pub kind: ServiceStateKind,
    pub state: ServiceStateDto,
    pub generation: u64,
}
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub enum ServiceStateKind { #[serde(rename = "service.state")] ServiceState }
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ServiceStateDto { Ready, StoreDegraded, Quiescing }
```

Control constructors fix schema version to `1`; their kind fields are single-value enums, and `ServiceStateDto` serializes as snake_case. None has an `id` field.

Use these transport-neutral result types so handlers can choose `200` versus `201` without inspecting domain internals:

```rust
pub type ApiResult<T> = Result<T, ApiError>;

pub enum CreateResult<T> {
    Created(T),
    Existing(T),
}

pub enum CancelResult {
    Finished(TaskDto),
    Accepted { task: TaskDto },
}

pub struct QuitAcceptance {
    trigger_after_response: Option<Box<dyn FnOnce() + Send + 'static>>,
}
```

`QuitAcceptance::take_trigger(&mut self) -> Option<Box<dyn FnOnce() + Send + 'static>>` moves the callback out exactly once.

The backend port must expose these exact operations:

```rust
#[async_trait::async_trait]
pub trait ApiBackend: Send + Sync + 'static {
    async fn bootstrap(&self, auth: &AuthContext) -> ApiResult<BootstrapResponse>;
    async fn list_repositories(&self, auth: &AuthContext) -> ApiResult<Vec<RepositoryDto>>;
    async fn add_repository(&self, auth: &AuthContext, request: AddRepositoryRequest) -> ApiResult<CreateResult<RepositoryDto>>;
    async fn pick_repository(&self, auth: &AuthContext) -> ApiResult<Option<CreateResult<RepositoryDto>>>;
    async fn list_tasks(&self, auth: &AuthContext, repository_id: Option<RepositoryId>) -> ApiResult<Vec<TaskDto>>;
    async fn create_task(&self, auth: &AuthContext, request: CreateTaskRequest) -> ApiResult<CreateResult<TaskDto>>;
    async fn task_detail(&self, auth: &AuthContext, id: TaskId) -> ApiResult<TaskDetailDto>;
    async fn cancel_task(&self, auth: &AuthContext, id: TaskId) -> ApiResult<CancelResult>;
    async fn retry_task(&self, auth: &AuthContext, id: TaskId) -> ApiResult<CreateResult<TaskDto>>;
    async fn task_events(&self, auth: &AuthContext, id: TaskId, after: i64) -> ApiResult<Vec<TaskEventDto>>;
    async fn request_quit(&self, auth: &AuthContext) -> ApiResult<QuitAcceptance>;
}
```

Keep the replay/live seam in the API crate without depending on app actors:

```rust
pub enum LiveEventItem {
    Event(TaskEventDto),
    Lagged,
}

pub type LiveEventStream = std::pin::Pin<
    Box<dyn futures_util::Stream<Item = LiveEventItem> + Send + 'static>,
>;
pub type ServiceStateStream = std::pin::Pin<
    Box<dyn futures_util::Stream<Item = ServiceStateControl> + Send + 'static>,
>;

#[async_trait::async_trait]
pub trait SseBackend: Send + Sync + 'static {
    fn subscribe_live(&self) -> LiveEventStream;
    fn subscribe_service_state(&self) -> ServiceStateStream;
    async fn current_service_state(&self) -> ApiResult<ServiceStateControl>;
    async fn latest_event_id(&self) -> ApiResult<i64>;
    async fn events_between(
        &self,
        after: i64,
        through: i64,
        limit: usize,
    ) -> ApiResult<Vec<TaskEventDto>>;
}
```

`events_between` returns only persisted IDs in `(after, through]`, ordered ascending. `LiveEventItem::Lagged` is a signal to refill from SQLite and is never serialized to the browser.

The request-security port is exact and HTTP-aware so it can reject duplicated raw headers before handler extraction:

```rust
pub struct AuthContext {
    pub session_id: String,
}

pub struct SessionExchange {
    pub set_cookie: http::HeaderValue,
}

#[async_trait::async_trait]
pub trait RequestSecurity: Send + Sync + 'static {
    async fn exchange(
        &self,
        parts: &http::request::Parts,
        token: &str,
    ) -> ApiResult<SessionExchange>;
    fn authorize_read(&self, parts: &http::request::Parts) -> ApiResult<AuthContext>;
    fn authorize_mutation(&self, parts: &http::request::Parts) -> ApiResult<AuthContext>;
    fn expected_public_origin(&self) -> &str;
}
```

Define internal and wire errors separately:

```rust
pub struct ApiError {
    pub status: http::StatusCode,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub details: std::collections::BTreeMap<String, serde_json::Value>,
}

pub struct ApiErrorResponse {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub request_id: String,
    pub details: std::collections::BTreeMap<String, serde_json::Value>,
}
```

Only Task 12's router injects request ID and serializes `ApiErrorResponse`. `CreateResult` and `CancelResult` are internal control results, never wrapper JSON: Created/Existing returns the inner DTO with `201`/`200`; Finished returns Task DTO with `200`; Accepted returns `{task,cancellation_requested:true}` with `202`. Store/domain failures are mapped to `ApiError` by app `ApplicationBackend`, without exposing secrets.

- [ ] **Step 4: Implement deterministic OpenAPI export**

`export_openapi` accepts exactly one output path argument, serializes `ApiDoc::openapi()` with pretty JSON plus one trailing newline, creates the parent directory, and writes through a uniquely named same-directory temporary file. Flush and `sync_all` the temporary file before publishing; never delete the destination first. A shared `atomic_replace` uses rename-over-existing on Unix and `MoveFileExW(MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)` through target-gated `windows-sys` on Windows, checks the OS result, and best-effort syncs the parent where supported. Clean up the temporary file on every failed publish. Task 12 switches the exporter to the real router-produced OpenAPI after paths exist.

- [ ] **Step 5: Run contract and workspace checks**

Run: `cargo test -p coding-agent-api --test openapi`

Run: `cargo run -p coding-agent-api --bin export_openapi -- web/openapi.json`

Run: `cargo run -p coding-agent-api --bin export_openapi -- target/openapi-check.json`

Run: `git diff --no-index --exit-code -- web/openapi.json target/openapi-check.json`

Run: `cargo test -p coding-agent-domain -p coding-agent-store -p coding-agent-api`

Expected: OpenAPI tests pass; `web/openapi.json` exists and the independent second export is byte-identical.

- [ ] **Step 6: Commit the API contract**

```bash
git add crates/coding-agent-api web/openapi.json Cargo.lock
git commit -m "feat: define local web api contract"
```

### Task 5: Serialize Writes and Own Service State

**Files:**
- Modify: `crates/coding-agent-app/Cargo.toml`
- Modify: `crates/coding-agent-app/src/lib.rs`
- Create: `crates/coding-agent-app/src/service_state.rs`
- Create: `crates/coding-agent-app/src/store_writer.rs`
- Create: `crates/coding-agent-app/tests/store_writer.rs`
- Create: `crates/coding-agent-app/tests/support/mod.rs`

**Interfaces:**
- Consumes: all store mutation methods from Tasks 2–3.
- Produces: `EventWake`, `StoreWriterHandle`, `StoreWriterError`, `WriteReceipt<T>`, `ServiceState::{Ready,StoreDegraded,Quiescing}`, `ServiceStateSnapshot { state, generation }`, and `ServiceStateController`.
- Invariant: application code outside `store_writer.rs` receives only a read-only `Store`; every mutation goes through `StoreWriterHandle`.

The app manifest delta for this task is exact:

```toml
[dependencies]
coding-agent-api = { path = "../coding-agent-api" }
coding-agent-domain = { path = "../coding-agent-domain" }
coding-agent-store = { path = "../coding-agent-store" }
thiserror.workspace = true
tokio.workspace = true
tokio-util.workspace = true
tracing.workspace = true

[dev-dependencies]
tempfile.workspace = true
```

- [ ] **Step 1: Write failing FIFO, transient retry, and generation tests**

```rust
#[tokio::test]
async fn writer_serializes_concurrent_creates() {
    let fixture = support::writer_fixture().await;
    let a = fixture.writer.create_task(support::new_task("a"));
    let b = fixture.writer.create_task(support::new_task("b"));
    let (a, b) = tokio::join!(a, b);
    assert!(a.unwrap().event_id < b.unwrap().event_id);
}

#[tokio::test]
async fn service_state_generation_never_moves_backwards() {
    let state = ServiceStateController::new(ServiceState::Ready);
    let a = state.set(ServiceState::StoreDegraded).unwrap();
    let b = state.set(ServiceState::Ready).unwrap();
    assert_eq!(a.generation + 1, b.generation);
    assert_eq!(state.current(), b);
}
```

Every app integration test starts with `mod support;`; `tests/support/mod.rs` owns fake clocks, actor fixtures, fault-controlled Store adapters, and builders shared by later tasks. Add a fault-injection test where two `SQLITE_BUSY` attempts precede a success, plus a test where a command deadline expires before a transaction attempt and the task remains uncommitted.

Use counting and panicking fake `EventWake` implementations to assert each committed task/event mutation notifies once, repository-only or rolled-back writes do not notify, and a wake panic cannot turn a durable commit into an API failure.

- [ ] **Step 2: Run the focused test and confirm red**

Run: `cargo test -p coding-agent-app --test store_writer`

Expected: compilation fails because the actor and service-state types do not exist.

- [ ] **Step 3: Implement the single writer actor**

`StoreWriterHandle` sends a closed `WriteCommand` enum over a bounded Tokio mpsc channel and awaits a oneshot. Include commands for repository registration, task create/retry, transition, running-event append, and incomplete recovery. The actor processes one command to completion before receiving another. Its constructor receives `std::sync::Arc<dyn EventWake>` through this Task-owned port:

```rust
pub trait EventWake: Send + Sync + 'static {
    fn wake(&self);
}
```

```rust
pub struct WriteReceipt<T> {
    pub value: T,
    pub event_id: Option<EventId>,
}

pub enum StoreWriterError {
    Busy,
    Store(coding_agent_store::StoreError),
    Closed,
}
```

Repository-only writes return `event_id = None`; a single task/event mutation returns its committed EventId, and bulk recovery returns `value.last_event_id`. The writer passes the full store RecoveryOutcome through unchanged so startup, degraded recovery, shutdown, and EventDispatcher use one high-watermark definition.

Retry only `SQLITE_BUSY` and `SQLITE_LOCKED` with delays of 25, 50, 100, 200, and 400 milliseconds. Each foreground command carries a deadline. Check it before the first transaction and before each retry; if it has expired after a failed/rolled-back attempt, return Busy as known-uncommitted. Once an attempt begins, never abandon its oneshot via an outer timeout: return its success or rolled-back failure, and rely on the original request ID/CAS if the HTTP client disconnected. A command uses that same request ID or CAS condition on every retry. After the store returns a committed task/event outcome, call the content-free `EventWake`; never send a TaskEvent object from the producer. Catch/log a wake implementation panic and still return the durable receipt because wakes are acceleration only; Task 6's periodic database poll is the loss-recovery path.

- [ ] **Step 4: Implement the single service-state publisher**

`ServiceStateController` owns one Tokio watch sender and a current snapshot protected by a mutex. `set(&self, next) -> Result<ServiceStateSnapshot, InvalidServiceTransition>` increments generation only when state changes. Legal edges are Ready ↔ StoreDegraded and either of those → Quiescing; same-state sets return the unchanged snapshot. Quiescing is terminal, so attempts to leave it return InvalidServiceTransition.

- [ ] **Step 5: Run focused and impacted suites**

Run: `cargo test -p coding-agent-app --test store_writer`

Run: `cargo test -p coding-agent-domain -p coding-agent-store -p coding-agent-app`

Expected: FIFO, retry, no-ambiguous-commit, and monotonic-generation tests pass.

- [ ] **Step 6: Commit the write and service-state actors**

```bash
git add crates/coding-agent-app Cargo.lock
git commit -m "feat: serialize application state writes"
```

### Task 6: Publish Persisted Events in Database Order

**Files:**
- Create: `crates/coding-agent-app/src/event_dispatcher.rs`
- Create: `crates/coding-agent-app/tests/event_dispatcher.rs`
- Modify: `crates/coding-agent-app/tests/support/mod.rs`
- Modify: `crates/coding-agent-app/src/lib.rs`

**Interfaces:**
- Consumes: `Store::events_after`, `Store::latest_event_id`, and Task 5's `EventWake` port.
- Produces: `EventDispatcherHandle::subscribe() -> broadcast::Receiver<TaskEvent>`, `EventDispatcherHandle::wake()`, `EventDispatcherHandle::flush_to(EventCursor)`, and `impl EventWake for EventDispatcherHandle`.
- Invariant: only this actor sends persisted TaskEvent values to the live broadcast channel.

- [ ] **Step 1: Write failing ordering and lost-wakeup tests**

```rust
#[tokio::test(start_paused = true)]
async fn dispatcher_reads_committed_events_in_id_order() {
    let fixture = support::dispatcher_fixture().await;
    let mut receiver = fixture.dispatcher.subscribe();
    fixture.commit_events_without_wake(&["first", "second"]).await;
    fixture.dispatcher.wake();
    let first = receiver.recv().await.unwrap();
    let second = receiver.recv().await.unwrap();
    assert!(first.id < second.id);
}

#[tokio::test(start_paused = true)]
async fn periodic_poll_recovers_a_lost_wakeup() {
    let fixture = support::dispatcher_fixture().await;
    let mut receiver = fixture.dispatcher.subscribe();
    fixture.commit_events_without_wake(&["lost-wakeup"]).await;
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert_eq!(receiver.recv().await.unwrap().kind(), TaskEventKind::ActivityAppended);
}
```

- [ ] **Step 2: Run the focused test and confirm red**

Run: `cargo test -p coding-agent-app --test event_dispatcher`

Expected: compilation fails on `EventDispatcherHandle`.

- [ ] **Step 3: Implement cursor-owned database polling**

Initialize the cursor from the startup-recovery high watermark. On wake or one-second interval, repeatedly query `events_after(cursor, 256)`, sort defensively by ID, skip IDs not greater than the cursor, broadcast each event, and update the cursor only after send. A send with zero receivers still advances because SQLite remains the replay source. `flush_to(target)` acknowledges only after the cursor reaches target or returns a store error.

- [ ] **Step 4: Run event and store suites**

Run: `cargo test -p coding-agent-app --test event_dispatcher`

Run: `cargo test -p coding-agent-store -p coding-agent-app`

Expected: ordered, duplicate-wakeup, lost-wakeup, and flush tests all pass.

- [ ] **Step 5: Commit the database-backed dispatcher**

```bash
git add crates/coding-agent-app
git commit -m "feat: publish durable events in order"
```

### Task 7: Implement TaskManager Claim, Cancel, and Quiesce Ownership

**Files:**
- Create: `crates/coding-agent-app/src/task_manager.rs`
- Create: `crates/coding-agent-app/tests/task_manager.rs`
- Modify: `crates/coding-agent-app/tests/support/mod.rs`
- Modify: `crates/coding-agent-app/src/lib.rs`

**Interfaces:**
- Consumes: `StoreWriterHandle`, read-only `Store`, `ServiceStateController`, and an injected `Arc<dyn TaskRunner>`.
- Produces: `TaskManagerHandle::{notify_queued,cancel,quiesce_and_interrupt}`, `CancelOutcome`, `TaskRunner`, `RunContext`, `RunnerEvent`, `RunnerEventSink`, `RunnerEventError`, `RunnerOutcome`, `RunnerShutdownHandle`, and `QuiesceResult`.
- Invariant: claim, cancel, runner event/result, reconciliation, and shutdown barrier are messages handled by one actor; create/retry stay serialized through StoreWriter and only notify this actor after commit.

Add `async-trait.workspace = true` to app dependencies in this task. Every test file imports the shared fixture with `mod support;`.

- [ ] **Step 1: Write failing permit and claim/cancel race tests**

```rust
#[tokio::test]
async fn fifth_task_stays_queued_until_a_permit_is_released() {
    let fixture = support::task_manager_fixture(4).await;
    let tasks = fixture.enqueue_blocking_tasks(5).await;
    fixture.wait_for_running(4).await;
    assert_eq!(fixture.load(tasks[4]).await.status, TaskStatus::Queued);
    fixture.finish(tasks[0]).await;
    fixture.wait_for_status(tasks[4], TaskStatus::Running).await;
}

#[tokio::test]
async fn running_is_never_visible_without_an_active_handle() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.pause_claim_after_handle_registration();
    let task = fixture.enqueue_blocking_tasks(1).await[0];
    fixture.wait_for_claim_pause().await;
    let manager = fixture.manager.clone();
    let cancel = tokio::spawn(async move { manager.cancel(task).await });
    fixture.wait_for_cancel_enqueued().await;
    fixture.resume_claim();
    let response = cancel.await.unwrap().unwrap();
    assert!(matches!(response, CancelOutcome::Accepted { .. } | CancelOutcome::Cancelled { .. }));
    fixture.assert_no_running_task_without_token().await;
}
```

Add an explicit cancel matrix paused (a) after permit acquisition but before provisional handle registration, (b) after handle registration but before Running commit, and (c) after Running commit but before runner spawn. No row may expose Running without a token: if queued cancel commits first no runner starts; if claim commits first, the registered token is triggered and the runner exits through the normal cancel result. Inject BUSY and terminal StoreWriter failure into the claim CAS and assert no runner spawns, the provisional handle is removed, the permit is released, Task remains Queued, and reconciliation later claims it exactly once. Also test queued cancel winning before claim, completed-vs-cancel first-commit wins, reconciliation after lost queue notification, FIFO `(created_at,id)`, late event rejection, and runner panic becoming `RUNNER_PANICKED` without affecting another task.

- [ ] **Step 2: Run focused tests and verify red**

Run: `cargo test -p coding-agent-app --test task_manager`

Expected: compilation fails because TaskManager and TaskRunner do not exist.

- [ ] **Step 3: Define the runner port and bounded event sink**

```rust
#[async_trait::async_trait]
pub trait TaskRunner: Send + Sync + 'static {
    async fn run(&self, context: RunContext, sink: RunnerEventSink) -> RunnerOutcome;
}

pub enum RunnerOutcome {
    Succeeded,
    Cancelled,
    Failed(TaskFailure),
}

pub enum CancelOutcome {
    Cancelled { task: Task },
    Accepted { task: Task },
}

pub enum RunnerEvent {
    PlanUpdated(PlanSnapshot),
    ActivityAppended(ActivityEntry),
    DiffUpdated(DiffSnapshot),
    TestUpdated(TestSnapshot),
}

pub enum RunnerEventError {
    TaskNotRunning,
    StoreDegraded,
    ManagerClosed,
}

pub struct RunnerEventSink {
    task_id: TaskId,
    sender: tokio::sync::mpsc::Sender<TaskManagerMessage>,
}

pub struct RunnerShutdownHandle {
    pub task_id: TaskId,
    pub cancellation: tokio_util::sync::CancellationToken,
    pub done: tokio::sync::oneshot::Receiver<()>,
}

pub enum QuiesceResult {
    Durable {
        recovery: coding_agent_store::RecoveryOutcome,
        active: Vec<RunnerShutdownHandle>,
    },
    Frozen {
        active: Vec<RunnerShutdownHandle>,
        error: StoreWriterError,
    },
}

pub struct RunContext {
    pub task: Task,
    pub repository: Repository,
    pub cancellation: tokio_util::sync::CancellationToken,
}
```

`RunnerEventSink` implements `pub async fn append(&self, event: RunnerEvent) -> Result<EventId, RunnerEventError>`: it sends only the four non-lifecycle variants through a bounded message back to the actor and awaits the oneshot persistence result without blocking a runtime thread. The actor persists it only through `append_running_event` and returns the committed event ID; a terminal task returns TaskNotRunning, degraded mode returns StoreDegraded, and a closed mailbox returns ManagerClosed. Runner lifecycle/terminal events are unrepresentable through this sink.

- [ ] **Step 4: Implement the actor ordering**

Scan queued tasks in `(created_at,id)` order. Use `Semaphore::try_acquire_owned`; if none is available, keep the task Queued and return to the mailbox. After acquiring: register provisional token/permit, perform the queued-to-running CAS through StoreWriter, spawn the runner only after commit, and clean up on CAS failure.

Cancel messages are decided by the same actor. Running cancel triggers the registered token, rereads the latest Task, and returns `Accepted`; queued cancel commits and returns `Cancelled`; already Cancelled returns the same `Cancelled`; Completed/Failed/Interrupted return `TaskManagerError::TaskNotCancellable`. Catch spawned runner panics through `JoinError`, persist one terminal result with `status = Running` CAS, then remove handle and release permit.

`quiesce_and_interrupt(deadline)` stops scans/claims, processes earlier mailbox messages, and performs one bulk incomplete-to-interrupted write through StoreWriter. It always freezes the actor: on commit it returns Durable with the Store recovery high watermark and active handles; on a rolled-back/deadline write error it returns Frozen with those same active handles for degraded shutdown. Each runner wrapper resolves its `done` receiver on success, failure, cancel, or panic.

- [ ] **Step 5: Run task manager and persistence regression suites**

Run: `cargo test -p coding-agent-app --test task_manager`

Run: `cargo test -p coding-agent-store -p coding-agent-app`

Expected: all race tests pass repeatedly; run the focused race test 25 times with `cargo test -p coding-agent-app --test task_manager running_is_never_visible_without_an_active_handle -- --test-threads=1` and observe zero failures.

- [ ] **Step 6: Commit the task-control actor**

```bash
git add crates/coding-agent-app Cargo.lock
git commit -m "feat: coordinate task lifecycle actor"
```

### Task 8: Add the Deterministic FakeTaskRunner

**Files:**
- Create: `crates/coding-agent-app/src/fake_runner.rs`
- Modify: `crates/coding-agent-app/src/lib.rs`
- Modify: `crates/coding-agent-app/tests/task_manager.rs`
- Modify: `crates/coding-agent-app/tests/support/mod.rs`

**Interfaces:**
- Consumes: Task 7 `TaskRunner`, `RunContext`, and `RunnerEventSink`.
- Produces: `FakeTaskRunner`, `FakeRunnerConfig`, and feature-gated `ScriptedFakeRunner` with `FakeScenario::{Success,Blocking,IgnoresCancellation,Failure,Panic}`.
- Invariant: production behavior is deterministic and never reads repository contents or opens the network.

Add the app Cargo feature now, before any test references scripted behavior:

```toml
[features]
default = []
test-support = []
```

- [ ] **Step 1: Write a failing deterministic-sequence test with paused time**

```rust
#[tokio::test(start_paused = true)]
async fn fake_runner_emits_the_approved_panel_sequence() {
    let fixture = support::fake_runner_fixture().await;
    let task = fixture.start().await;
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    fixture.wait_for_terminal(task.id).await;
    assert_eq!(
        fixture.event_kinds(task.id).await,
        vec![
            TaskEventKind::TaskQueued,
            TaskEventKind::TaskStarted,
            TaskEventKind::PlanUpdated,
            TaskEventKind::ActivityAppended,
            TaskEventKind::ActivityAppended,
            TaskEventKind::ActivityAppended,
            TaskEventKind::DiffUpdated,
            TaskEventKind::TestUpdated,
            TaskEventKind::TestUpdated,
            TaskEventKind::TaskCompleted,
        ]
    );
}
```

- [ ] **Step 2: Run the focused test and verify red**

Run: `cargo test -p coding-agent-app --features test-support fake_runner_emits_the_approved_panel_sequence`

Expected: compilation fails because `FakeTaskRunner` does not exist.

- [ ] **Step 3: Implement success, cancellation, failure, and panic scripts**

The success runner emits one complete three-item plan, three stable activity entries, one complete synthetic diff snapshot, tests Running, then tests Passed, with a configurable 200-millisecond interval between emissions. Check the cancellation token before and after every interval. The production constructor always selects Success.

Under Cargo feature `test-support`, `ScriptedFakeRunner` consumes a process-loaded queue of explicit scenarios by task creation order. It does not inspect prompt text and exposes no HTTP control route. Blocking waits on cancellation or a test release channel; IgnoresCancellation waits only on its test release channel so shutdown budgets can be proved; Failure returns a fixed `FAKE_RUNNER_FAILURE`; Panic deliberately panics for isolation tests.

- [ ] **Step 4: Run fake-runner and task-manager tests**

Run: `cargo test -p coding-agent-app --features test-support fake_runner`

Run: `cargo test -p coding-agent-app --features test-support --test task_manager`

Expected: deterministic sequence, cancel, failure, and panic isolation tests pass without wall-clock sleeps.

- [ ] **Step 5: Commit the fake execution slice**

```bash
git add crates/coding-agent-app
git commit -m "feat: add deterministic fake task runner"
```

### Task 9: Coordinate StoreDegraded Recovery

**Files:**
- Create: `crates/coding-agent-app/src/shutdown.rs`
- Create: `crates/coding-agent-app/tests/degraded_recovery.rs`
- Modify: `crates/coding-agent-app/src/store_writer.rs`
- Modify: `crates/coding-agent-app/src/task_manager.rs`
- Modify: `crates/coding-agent-app/src/lib.rs`
- Modify: `crates/coding-agent-app/tests/support/mod.rs`

**Interfaces:**
- Consumes: store `RecoveryOutcome` and its durable `high_watermark`.
- Produces: `DegradedCoordinator::run`, `PendingDurableResult`, and app-level `DegradedRecoveryResult`.
- Invariant: after a background write exhausts retry, no new runner starts until all ambiguous queued/running tasks are durably Interrupted and service state returns to Ready.

```rust
pub enum PendingDurableResult {
    RunnerEvent { task_id: TaskId, event: RunnerEvent },
    RunnerTerminal { task_id: TaskId, outcome: RunnerOutcome },
}

pub struct DegradedRecoveryResult {
    pub recovery: coding_agent_store::RecoveryOutcome,
    pub discarded_pending_count: usize,
    pub ready_generation: u64,
}
```

Pending values are diagnostics/ownership markers, not a second durable queue. Once bulk recovery commits Interrupted for every ambiguous task and dispatcher flushes through `recovery.high_watermark`, the coordinator discards them and returns the generation produced by setting Ready.

- [ ] **Step 1: Write a failing background terminal-write test**

```rust
#[tokio::test(start_paused = true)]
async fn terminal_write_failure_stops_claims_until_recovery() {
    let fixture = support::degraded_fixture_with_concurrency(1).await;
    let running = fixture.start_success_task().await;
    let queued = fixture.enqueue_task().await;
    fixture.fail_all_background_writes();
    fixture.finish_runner(running).await;
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    assert_eq!(fixture.load(queued).await.status, TaskStatus::Queued);
    fixture.restore_writes();
    fixture.wait_for_state(ServiceState::Ready).await;
    assert_eq!(fixture.load(running).await.status, TaskStatus::Interrupted);
}
```

- [ ] **Step 2: Run the focused test and verify red**

Run: `cargo test -p coding-agent-app --test degraded_recovery`

Expected: the test fails because a background StoreWriter error is not coordinated.

- [ ] **Step 3: Implement degraded entry and recovery ordering**

When runner-event or terminal persistence exhausts bounded retries, retain the pending result in memory, set StoreDegraded, stop reconciliation/claim, and cancel all active tokens. Retry a single bulk `recover_incomplete` transaction every second through StoreWriter. Only after it commits and EventDispatcher flushes its high watermark may the coordinator clear pending results, restart reconciliation, and set Ready with a larger generation.

Foreground mutation timeout returns `503 STORE_BUSY` without entering this coordinator when the command is known uncommitted. Non-transient corruption remains StoreDegraded and never deletes/recreates the database.

- [ ] **Step 4: Run degraded, manager, writer, and dispatcher suites**

Run: `cargo test -p coding-agent-app --test degraded_recovery --test store_writer --test event_dispatcher --test task_manager`

Expected: recovery order is Interrupted event committed → dispatcher flushed → Ready generation published; no queued task starts while degraded.

- [ ] **Step 5: Commit the degraded-mode coordinator**

```bash
git add crates/coding-agent-app
git commit -m "feat: recover from task store outages"
```

### Task 10: Add Cross-Platform Paths, Repository Discovery, and Native Adapters

**Files:**
- Modify: `crates/coding-agent-app/Cargo.toml`
- Modify: `crates/coding-agent-app/src/lib.rs`
- Create: `crates/coding-agent-app/src/platform.rs`
- Create: `crates/coding-agent-app/src/repository_service.rs`
- Create: `crates/coding-agent-app/src/native_dialog.rs`
- Create: `crates/coding-agent-app/tests/platform.rs`
- Create: `crates/coding-agent-app/tests/repository_service.rs`
- Modify: `crates/coding-agent-app/tests/support/mod.rs`

**Interfaces:**
- Produces: `PlatformPaths`, `PrivateFile`, `BrowserLauncher`, `CommandRunner`, `RepositoryDiscovery`, `DiscoveredRepository`, `NativeDialogService`, and `PickerError`.
- Invariant: discovery may run only `git rev-parse` and `cargo locate-project`; it never reads source contents, resolves dependencies, builds code, or changes a repository.

Add this app manifest delta:

```toml
[dependencies]
directories.workspace = true
rfd.workspace = true
webbrowser.workspace = true

[target.'cfg(windows)'.dependencies]
windows-sys.workspace = true
```

- [ ] **Step 1: Write failing path, permission, discovery, and picker tests**

Create temporary repositories covering a nested selected directory, a manifest between selected and Git root, a Cargo workspace outside Git root, a missing manifest, missing/stale/dirty `Cargo.lock` states, a repository `rust-toolchain.toml` naming an intentionally unavailable channel, a symlinked selection, a nonexistent selected path, and an ordinary file selected as though it were a directory. Assert the last two fail before either command is invoked, with `REPOSITORY_PATH_NOT_FOUND` and `REPOSITORY_PATH_NOT_DIRECTORY` respectively. Record the recursive relative file list, every existing lockfile byte sequence, and `git status --porcelain=v1` before discovery and assert byte-for-byte equality afterward. The unavailable-toolchain fixture must still locate successfully, proving Cargo ran from the neutral runtime cwd rather than activating the repository override.

```rust
#[tokio::test]
async fn discovery_is_read_only_and_uses_the_nearest_manifest() {
    let fixture = support::nested_workspace_fixture().await;
    let before = fixture.repository_fingerprint().await;
    let found = fixture.discovery.discover(&fixture.selected).await.unwrap();
    assert_eq!(found.git_root, fixture.git_root.canonicalize().unwrap());
    assert_eq!(found.cargo_workspace_root, fixture.workspace_root.canonicalize().unwrap());
    assert_eq!(before, fixture.repository_fingerprint().await);
}

#[tokio::test]
async fn a_second_picker_is_rejected_while_the_first_is_open() {
    let fixture = support::blocking_picker_fixture();
    let first_service = fixture.service.clone();
    let first = tokio::spawn(async move { first_service.pick_repository().await });
    fixture.wait_until_open().await;
    assert!(matches!(fixture.service.pick_repository().await, Err(PickerError::AlreadyOpen)));
    fixture.cancel();
    assert_eq!(first.await.unwrap().unwrap(), None);
}
```

`platform.rs` tests must assert data/runtime directories and sensitive files are private: Unix modes `0700`/`0600`; Windows owner DACL grants the current user and rejects inherited broad access. Browser-launch failure must be observable without becoming a process-fatal error.

- [ ] **Step 2: Run focused tests and verify red**

Run: `cargo test -p coding-agent-app --test platform --test repository_service`

Expected: compilation fails because the platform and discovery ports do not exist.

- [ ] **Step 3: Implement application paths and private-file helpers**

`PlatformPaths::discover()` uses `directories::ProjectDirs::from("com", "ngy", "coding-agent")` for user-local data. Prefer the OS runtime directory when available; otherwise use `<data_local>/run`. It exposes `database_path`, permanent `instance.lock`, replaceable `instance.json`, and `unclean-shutdown.json`. Directory creation is idempotent. Sensitive file creation uses `create_new`, applies owner-only permissions before publishing content, and never follows a symlink at the final path. Windows permission code is isolated behind `cfg(windows)` and uses `windows-sys`; Unix uses `OpenOptionsExt` and `PermissionsExt`.

- [ ] **Step 4: Implement read-only repository discovery**

Before launching any child process, inspect the selected path with `symlink_metadata`/`metadata`: map absence to `REPOSITORY_PATH_NOT_FOUND`, reject a non-directory as `REPOSITORY_PATH_NOT_DIRECTORY`, then canonicalize and normalize the directory. Run exactly this flow through an injectable `CommandRunner`:

1. `git -C <selected> rev-parse --show-toplevel`.
2. Walk ancestors from selected through the normalized Git root and choose the first existing `Cargo.toml`.
3. From `PlatformPaths::runtime_dir` as the neutral child-process working directory, run `cargo locate-project --workspace --manifest-path <manifest> --message-format plain`.
4. Normalize the returned manifest parent and reject it unless path-component containment proves it is inside the Git root.

Do not use string-prefix containment. Convert command spawn failures, non-zero exits, invalid UTF-8, missing roots, and out-of-root workspaces into stable codes without returning raw stderr to API callers. Unit tests prove invalid selected paths invoke neither Git nor Cargo. The integration test uses real Git/Cargo and confirms no file changes.

- [ ] **Step 5: Implement browser and serialized native-dialog adapters**

`BrowserLauncher::open` delegates only complete `http://127.0.0.1:<port>/#token=<token>` URLs to hardened `webbrowser`. A failure returns the URL for the caller's native error dialog and does not stop the server. `NativeDialogService` owns one atomic/mutex gate and calls `rfd` through its platform-supported async adapter, including the required main-thread/event-loop handoff on macOS. Cancellation is `Ok(None)`, concurrent entry is `PickerError::AlreadyOpen`, and handlers never call `rfd` directly. Fix and test the stable discovery/dialog codes `REPOSITORY_PATH_NOT_FOUND`, `REPOSITORY_PATH_NOT_DIRECTORY`, `CARGO_WORKSPACE_NOT_FOUND`, `CARGO_WORKSPACE_OUTSIDE_GIT_ROOT`, `REPOSITORY_COMMAND_FAILED`, and `PICKER_ALREADY_OPEN`.

- [ ] **Step 6: Run platform regressions and commit**

Run: `cargo test -p coding-agent-app --test platform --test repository_service`

Run: `cargo test -p coding-agent-store -p coding-agent-app`

Expected: all platform and discovery tests pass, and the real fixture fingerprint remains unchanged.

```bash
git add crates/coding-agent-app Cargo.lock
git commit -m "feat: add local platform and repository discovery"
```

### Task 11: Implement Process-Scoped Session, Host, Origin, and CSRF Security

**Files:**
- Create: `crates/coding-agent-app/src/security.rs`
- Create: `crates/coding-agent-app/tests/security.rs`
- Modify: `crates/coding-agent-app/src/lib.rs`
- Modify: `crates/coding-agent-app/tests/support/mod.rs`

**Interfaces:**
- Produces: `SecuritySeed`, `SecurityManager`, `LaunchToken`, `LauncherSecret`, `SessionRecord`, and the app implementation of API `RequestSecurity`.
- Invariant: all secrets live only in memory except the launcher secret in the owner-only runtime descriptor; none enter SQLite, request targets, or ordinary logs.

Add `axum-extra.workspace = true`, `base64.workspace = true`, `getrandom.workspace = true`, `http.workspace = true`, and `subtle.workspace = true` to app dependencies, plus `tracing-subscriber.workspace = true` to app dev-dependencies for the redaction capture.

- [ ] **Step 1: Write failing one-time token and request-boundary tests**

```rust
#[tokio::test]
async fn concurrent_exchange_consumes_a_launch_token_once() {
    let security = support::security_fixture();
    let token = security.issue_launch_token();
    let (a, b) = tokio::join!(
        security.exchange(&token, support::valid_exchange_request()),
        security.exchange(&token, support::valid_exchange_request()),
    );
    assert_eq!([a.is_ok(), b.is_ok()].into_iter().filter(|ok| *ok).count(), 1);
}
```

Add table tests for exact Host, missing/foreign Origin, missing/wrong CSRF, forged/old cookie, two-minute expiry using a fake clock, process restart invalidation, public read authorization, mutation authorization, and launcher-secret constant-time comparison. Capture tracing output seeded with known token, launcher secret, cookie, and CSRF and assert ordinary info logs contain none of their bytes. Assert responses contain no `Access-Control-Allow-Origin` header.

- [ ] **Step 2: Run the focused suite and verify red**

Run: `cargo test -p coding-agent-app --test security`

Expected: compilation fails because `SecurityManager` and the `RequestSecurity` implementation do not exist.

- [ ] **Step 3: Implement secret issuance and atomic exchange**

Generate every launch token, launcher secret, session ID, and CSRF token from 32 bytes filled by `getrandom`, then encode with URL-safe base64 without padding. `SecuritySeed::generate` creates process secrets before port binding; `SecurityManager::from_seed(seed, public_origin, clock)` consumes it exactly once after the loopback port is known. Store launch tokens in one mutex-protected map with issued and expiry instants. Exchange validates exact Host and configured public Origin, removes a valid token while holding the map lock, then creates an independent session. Use `subtle::ConstantTimeEq` for presented secret/token comparison.

Return a host-only `coding_agent_session` cookie with `HttpOnly`, `SameSite=Strict`, and `Path=/`; omit `Domain`, `Expires`, and `Secure` because production uses loopback HTTP. JavaScript receives the CSRF token only from authenticated bootstrap. A fresh `SecurityManager` has no knowledge of any earlier process token, cookie, CSRF value, or launcher secret.

- [ ] **Step 4: Implement the three authorization levels**

Implement API `RequestSecurity` as:

- exchange: exact Host, exact public Origin, and one valid launch token;
- read/SSE: exact Host and one live session cookie;
- mutation: read checks plus exact Origin and constant-time `X-CSRF-Token` match.

Internal `/_local/ready` and `/_local/reopen` use exact Host plus `X-Launcher-Secret`, never the browser cookie. Reject duplicated security headers, non-loopback configured origins in production, and any Host alias such as `localhost`. Request diagnostics may record a generated request ID and stable error code only.

Development mode accepts only its one explicitly configured Vite public Origin and proxy Host. It executes the same session, CSRF, launcher-secret, and mutation-gate checks as production; there is no debug authentication bypass or wildcard `localhost` rule.

- [ ] **Step 5: Run security and contract suites**

Run: `cargo test -p coding-agent-app --test security`

Run: `cargo test -p coding-agent-api -p coding-agent-app`

Expected: the concurrent-exchange test has one success; every negative matrix row fails closed.

- [ ] **Step 6: Commit the security boundary**

```bash
git add crates/coding-agent-app Cargo.lock
git commit -m "feat: protect local browser sessions"
```

### Task 12: Wire the Protected REST API and Application Backend

**Files:**
- Create: `crates/coding-agent-api/src/router.rs`
- Create: `crates/coding-agent-api/tests/router.rs`
- Create: `crates/coding-agent-api/tests/support/mod.rs`
- Create: `crates/coding-agent-app/src/server.rs`
- Create: `crates/coding-agent-app/tests/server.rs`
- Modify: `crates/coding-agent-api/src/lib.rs`
- Modify: `crates/coding-agent-app/src/lib.rs`
- Modify: `crates/coding-agent-app/tests/support/mod.rs`

**Interfaces:**
- Produces: `build_api_router`, `ApplicationBackend`, `MutationGate`, and exact REST-to-domain/store error mapping.
- Invariant: route handlers contain transport mapping only; mutations go through TaskManager/StoreWriter and repository discovery goes through Task 10 services.

Add `http-body-util.workspace = true`, `tokio.workspace = true`, and `tower.workspace = true` to API dev-dependencies. Add `axum.workspace = true`, `http-body-util.workspace = true`, `tower.workspace = true`, and `tower-http.workspace = true` to app dependencies; `http` is already a direct app dependency from Task 11. Every API integration test begins with `mod support;`; that module owns fake backend/security/SSE ports and response decoders.

- [ ] **Step 1: Write failing route matrices with fake ports**

The API router test supplies fake `ApiBackend`, `RequestSecurity`, and `SseBackend` implementations and covers every route, method, authentication level, content type, request ID, and success/error status. Assert the router-produced OpenAPI contains exactly the approved `/api/session/exchange`, bootstrap, repository, task/detail/cancel/retry/task-events, global events, and app quit paths; cancel documents `200 TaskDto` and `202 CancellationAcceptedResponse`. Include these mutation assertions:

- repository create/pick: `201` created, `200` existing, picker cancel `204`, picker busy `409`;
- task create: `201` first, `200` equivalent idempotent replay, `409` conflicting replay, `422` blank or over 50,000 Unicode scalars;
- cancel: Queued `200`, Running `202`, Cancelled `200`, other terminal `409`;
- retry: first child `201`, same direct child `200`, non-terminal `409`;
- bounded SQLite BUSY/LOCKED exhaustion: `503 {code:"STORE_BUSY",retryable:true}` with no committed mutation;
- accepted quit: `202 {"status":"shutting_down"}`; closed/degraded data-mutation gate: stable `503`.

Add a concurrent same-request-ID test proving only one task/event is created and both responses reference the same Task.

Add a concurrent retry test proving many requests against one terminal source return the same direct child with one `201` and the rest `200`. While service state is StoreDegraded, verify repository/task mutations return `503 STORE_DEGRADED` but the protected quit endpoint remains available so the user can enter degraded shutdown; tests may seed Store state directly but may not call a bypassing public enqueue path.

Capture server info logs for requests containing a known prompt and canonical path; assert logs contain only stable request/repository/task IDs and error codes, not the prompt or full path. Explicit local debug logging may treat paths as user data but still never emits session secrets.

- [ ] **Step 2: Run route tests and verify red**

Run: `cargo test -p coding-agent-api --test router`

Expected: compilation fails because the router is not defined.

- [ ] **Step 3: Implement the API router from the OpenAPI-bearing handlers**

Build routes with `utoipa-axum` so the runtime handler path/method and exported OpenAPI derive from the same registration. `api_openapi()` constructs the same unbound `OpenApiRouter<ApiState>` and returns its document; `build_api_router` supplies state and serves it; `export_openapi` now calls `api_openapi()` instead of the component-only Task 4 document. Apply exact Host validation outside the entire router; apply read or mutation authorization per endpoint. `POST /api/session/exchange` succeeds with `204` plus `Set-Cookie`. Every response, including rejections and panics, includes `X-Request-Id`; never reflect a malformed incoming ID.

Map `CreateResult` to `201`/`200`. Map stable app errors to the approved JSON envelope and never include command stderr, secrets, prompt text, or filesystem internals beyond the already-authorized repository DTO. Do not add CORS middleware.

- [ ] **Step 4: Implement `ApplicationBackend` and mutation-gate entry**

`ApplicationBackend` maps bootstrap/list/detail/events to one read-only Store and resolves the authenticated session's CSRF through SecurityManager. It sends create/retry through StoreWriter and cancel through TaskManager. Trim and count prompt Unicode scalar values before enqueueing. A successful create/retry notifies TaskManager only after commit; a lost notification is tolerated by reconciliation. Repository path and picker routes share RepositoryDiscovery and StoreWriter registration. All store/domain errors become API-owned errors here, never in the API crate.

`MutationGate::enter_data_mutation()` returns an RAII guard while Ready, STORE_DEGRADED while degraded, and APP_SHUTTING_DOWN once closed. `prepare_quit()` is allowed in Ready or StoreDegraded and rejects only after Quiescing begins. Quit returns its `202` body through a response-body wrapper whose end-of-stream callback sends the shutdown signal; an integration test must receive the full response before the listener begins quiescing.

- [ ] **Step 5: Run API, server, manager, and store suites**

Run: `cargo test -p coding-agent-api --test router`

Run: `cargo test -p coding-agent-app --test server --test task_manager --test store_writer`

Run: `cargo run -p coding-agent-api --bin export_openapi -- web/openapi.json`

Run: `cargo run -p coding-agent-api --bin export_openapi -- target/openapi-check.json`

Run: `git diff --no-index --exit-code -- web/openapi.json target/openapi-check.json`

Expected: all route matrices pass; the router path additions intentionally update the tracked contract, and two fresh exports are byte-identical.

- [ ] **Step 6: Commit the protected command/query layer**

```bash
git add crates/coding-agent-api crates/coding-agent-app web/openapi.json Cargo.lock
git commit -m "feat: expose protected local rest api"
```

### Task 13: Implement Gap-Free SSE Replay and Live Streaming

**Files:**
- Create: `crates/coding-agent-api/src/sse.rs`
- Create: `crates/coding-agent-api/tests/sse.rs`
- Modify: `crates/coding-agent-api/src/router.rs`
- Modify: `crates/coding-agent-api/src/lib.rs`
- Modify: `crates/coding-agent-app/src/server.rs`
- Modify: `crates/coding-agent-app/tests/server.rs`
- Modify: `crates/coding-agent-api/tests/support/mod.rs`
- Modify: `crates/coding-agent-app/tests/support/mod.rs`

**Interfaces:**
- Consumes: Task 4 `SseBackend`, Task 6 `EventDispatcherHandle`, read-only Store replay, and `ServiceStateController` watch.
- Produces: authenticated `GET /api/events?after=<id>` with persisted task frames and non-persisted control frames.
- Invariant: the last emitted persisted ID is strictly increasing; service-state controls and heartbeats never change it.

Add `async-stream.workspace = true` and `tokio.workspace = true` to API dependencies. Add `futures-util.workspace = true`, `tokio-stream.workspace = true`, and `async-stream.workspace = true` to app dependencies for the port adapter.

- [ ] **Step 1: Write failing join, overlap, reset, lag, and heartbeat tests**

Use a deterministic fake `SseBackend` that can pause between subscription, high-watermark read, backlog pages, and live drain. Cover an event committed in each pause, the same event appearing in backlog and live, an out-of-order live buffer, broadcast lag, a cursor greater than database maximum, service generation changing between bootstrap and SSE, and a 15-second paused-time heartbeat.

```rust
#[tokio::test]
async fn subscribe_before_backlog_has_no_gap_or_duplicate() {
    let fixture = support::join_fixture(40);
    fixture.pause_after_live_subscribe();
    let connect_fixture = fixture.clone();
    let connecting = tokio::spawn(async move { connect_fixture.connect().await });
    fixture.wait_for_join_pause().await;
    fixture.commit(41).await;
    fixture.resume();
    let stream = connecting.await.unwrap();
    fixture.commit(42).await;
    assert_eq!(stream.take_persisted_ids(2).await, vec![41, 42]);
}
```

- [ ] **Step 2: Run SSE tests and verify red**

Run: `cargo test -p coding-agent-api --test sse`

Expected: compilation fails because the SSE join is not implemented.

- [ ] **Step 3: Implement initial service control and persistent join**

After read authorization, subscribe to service-state and live task streams first. Immediately emit the current `ServiceStateControl`. Read current maximum ID; if `after` is greater, emit `event: stream.reset` without an `id` field and close. Otherwise page `events_between(after, high, 256)`, emit ascending IDs through `high`, sort/deduplicate buffered live items, then continue live while skipping every ID not greater than the last sent.

Wire format uses persisted `id`, the domain event name, and one-line JSON data. `stream.reset` and `service.state` have no persisted ID. Unknown internal errors terminate the stream after a diagnostic log without serializing secrets.

- [ ] **Step 4: Recover broadcast lag and merge service state**

On `LiveEventItem::Lagged`, query the new maximum and refill SQLite pages from the last sent ID before consuming live again. Continue until caught up; never synthesize task events. Coalesce service-state watch changes and emit only generations greater than the last service generation. Interleave a `: heartbeat` comment every 15 seconds without starving either source.

- [ ] **Step 5: Run focused and process-level SSE regressions**

Run: `cargo test -p coding-agent-api --test sse`

Run: `cargo test -p coding-agent-app --test event_dispatcher --test server`

Expected: persisted output is strictly increasing and gap-free under backlog/live overlap and lag; reset closes; heartbeat carries no ID.

- [ ] **Step 6: Commit replayable SSE**

```bash
git add crates/coding-agent-api crates/coding-agent-app Cargo.lock
git commit -m "feat: stream replayable task events"
```

### Task 14: Compose Primary and Secondary Single-Instance Startup

**Files:**
- Create: `crates/coding-agent-app/src/single_instance.rs`
- Create: `crates/coding-agent-app/src/main.rs`
- Create: `crates/coding-agent-app/tests/single_instance.rs`
- Modify: `crates/coding-agent-app/src/platform.rs`
- Modify: `crates/coding-agent-app/src/server.rs`
- Modify: `crates/coding-agent-app/src/lib.rs`
- Modify: `crates/coding-agent-app/tests/support/mod.rs`

**Interfaces:**
- Produces: `InstanceLock`, `RuntimeDescriptor`, `PrimaryRuntime`, `SecondaryRuntime`, `StartupPhase`, `/_local/ready`, and `/_local/reopen`.
- Invariant: lock ownership is decided before SQLite is opened; a secondary process never constructs StoreWriter or TaskManager.

Add `serde.workspace = true`, `serde_json.workspace = true`, `time.workspace = true`, and `uuid.workspace = true` to app dependencies, and promote the existing `tracing-subscriber.workspace = true` entry from app dev-dependencies to dependencies.

- [ ] **Step 1: Write failing lock, descriptor, and startup-phase tests**

Test the phase matrix: application-data directory creation/permission failure, lock held before descriptor publication, descriptor published while readiness says Starting, Ready reopen, malformed descriptor, wrong launcher secret, descriptor for a dead process, browser open failure, and 10-second timeout. The unwritable-path test injects the platform filesystem error, asserts one native error message, non-zero exit, and no lock/database/listener. Assert the secondary opens no database connection by injecting a Store factory that panics if called.

```rust
#[tokio::test(start_paused = true)]
async fn secondary_waits_for_atomic_descriptor_without_opening_store() {
    let fixture = support::startup_fixture();
    let primary_lock = fixture.hold_primary_lock();
    let secondary_fixture = fixture.clone();
    let secondary = tokio::spawn(async move { secondary_fixture.start_secondary().await });
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    fixture.publish_ready_descriptor(&primary_lock).await;
    assert!(secondary.await.unwrap().unwrap().browser_opened);
    fixture.assert_store_factory_unused();
}
```

- [ ] **Step 2: Run single-instance tests and verify red**

Run: `cargo test -p coding-agent-app --test single_instance`

Expected: compilation fails because lock and descriptor types do not exist.

- [ ] **Step 3: Implement permanent lock-file ownership and atomic descriptor publication**

Open the permanent `instance.lock` with read/write/create and call stable `std::fs::File::try_lock`. Keep that file descriptor alive for the full primary lifetime; never delete or rename the lock file. Only a lock owner may remove a stale descriptor.

Publish `RuntimeDescriptor { instance_id, pid, port, started_at, launcher_secret }` to a private sibling temporary file, call `sync_all`, atomically rename to `instance.json`, and sync the parent directory where supported. Readers reopen after every retry and never read the temporary path. Validate field bounds, loopback port, UUID, PID, and owner-only permissions before contacting a primary.

- [ ] **Step 4: Implement exact primary composition order**

The primary performs: paths → lock → stale descriptor cleanup → Store open/migrate → atomic incomplete recovery → generate SecuritySeed → bind `127.0.0.1:0` → construct SecurityManager with the exact bound origin → initialize dispatcher at recovered high watermark → start StoreWriter/TaskManager → serve Starting mode → self-probe `/_local/ready` with launcher secret → set Ready → publish descriptor → open the fragment URL.

Application-data/runtime creation or permission failure displays a native error and exits before lock/database/listener. Migration/recovery failure displays a native error and exits before listener publication. Bind retries are finite. Browser failure keeps the server alive and shows the complete copyable URL through the native message adapter. Starting mode exposes only the launcher-protected ready probe; public API and static requests return `503 APP_STARTING`.

- [ ] **Step 5: Implement secondary reopen without a second writer**

On lock contention, retry descriptor read and launcher-protected readiness/reopen with bounded exponential delays totaling at most 10 seconds. `/_local/ready` returns only instance ID and state. `/_local/reopen` returns `503` until Ready; when Ready it issues a new two-minute one-time browser token and returns the full fragment URL. Open that URL and exit zero. If the locked primary cannot be verified, show an explicit error and leave lock/descriptor untouched.

- [ ] **Step 6: Run startup and actor regression suites**

Run: `cargo test -p coding-agent-app --test single_instance --test server --test task_manager --test event_dispatcher`

Expected: all phase interleavings pass; secondary tests prove no Store construction.

- [ ] **Step 7: Commit the executable composition root**

```bash
git add crates/coding-agent-app Cargo.lock
git commit -m "feat: launch one protected local instance"
```

### Task 15: Implement Graceful and Degraded Shutdown

**Files:**
- Modify: `crates/coding-agent-app/src/shutdown.rs`
- Modify: `crates/coding-agent-app/src/server.rs`
- Modify: `crates/coding-agent-app/src/task_manager.rs`
- Modify: `crates/coding-agent-app/src/event_dispatcher.rs`
- Modify: `crates/coding-agent-app/src/single_instance.rs`
- Create: `crates/coding-agent-app/tests/shutdown.rs`
- Modify: `crates/coding-agent-app/tests/support/mod.rs`

**Interfaces:**
- Produces: `ShutdownCoordinator`, `ShutdownOutcome::{Clean,Degraded}`, and signal/HTTP shutdown sources.
- Invariant: persistent shutdown gets at most 5 seconds and the entire shutdown gets at most 10 seconds; descriptor and lock cleanup is attempted on every path.

- [ ] **Step 1: Write failing quiesce interleaving and budget tests**

Pause create, retry, claim, runner event, runner result, and quit at each gate/actor boundary. Assert an operation that entered before gate close either commits before the TaskManager barrier or fails deterministically; after the barrier, no late write can leave Queued/Running. Add a permanently failing Store test where marker creation also fails and assert virtual time reaches process-exit decision by 10 seconds with descriptor removed and lock released. Run the clean-store path with `FakeScenario::IgnoresCancellation`: the durable barrier must first persist Interrupted, waiting for `done` must stop at the remaining total budget, and the process must still choose exit by 10 seconds.

```rust
#[tokio::test(start_paused = true)]
async fn permanent_store_failure_cannot_block_exit() {
    let fixture = support::shutdown_fixture_with_unwritable_store_and_marker();
    let coordinator = fixture.coordinator.clone();
    let shutdown = tokio::spawn(async move { coordinator.shutdown().await });
    tokio::time::advance(std::time::Duration::from_secs(10)).await;
    assert_eq!(shutdown.await.unwrap(), ShutdownOutcome::Degraded);
    fixture.assert_listener_closed();
    fixture.assert_descriptor_removed();
    fixture.assert_lock_released();
}
```

- [ ] **Step 2: Run the focused suite and verify red**

Run: `cargo test -p coding-agent-app --features test-support --test shutdown`

Expected: shutdown ordering/budget assertions fail because no coordinator owns the complete sequence.

- [ ] **Step 3: Implement normal quiesce ordering**

Accept Ctrl-C, OS termination, or the deferred Web UI quit signal once. Set service state Quiescing, close MutationGate, wait for all existing guards, then send TaskManager `quiesce_and_interrupt(deadline)` as a FIFO barrier. Match `QuiesceResult::Durable`, cancel its active tokens, wait on their done receivers only within the remaining budget, reject late event/result CAS, flush EventDispatcher through `recovery.high_watermark`, checkpoint/close SQLite handles, stop accepting HTTP, atomically remove descriptor, and drop the locked file. Match Frozen or a 5-second stage timeout by entering the fallback in Step 4.

Preserve Completed/Failed committed before the barrier. Any task still Queued/Running in the bulk shutdown transaction becomes Interrupted, never Cancelled. Closing or refreshing a browser produces no shutdown signal.

- [ ] **Step 4: Implement degraded fallback and diagnostic marker**

Cap the persistence stage at 5 seconds. If it expires or Store is permanently broken, freeze TaskManager in memory, cancel all tokens, close event sinks/listener, and best-effort write a private marker containing only timestamp, instance ID, and stable error code. Whether marker creation succeeds or fails, remove the descriptor, release the lock, and select a non-zero exit code before the 10-second total deadline.

Every next primary startup runs the normal incomplete recovery regardless of marker presence. Remove the marker only after recovery commits. A database open/migration failure leaves both database and marker untouched and exits without a restart loop. Before degraded exit, publish/log a user-visible stable message stating that some terminal task states could not be persisted; never claim a clean shutdown.

- [ ] **Step 5: Run shutdown and recovery regressions**

Run: `cargo test -p coding-agent-app --features test-support --test shutdown --test degraded_recovery --test task_manager --test server --test single_instance`

Expected: every interleaving is terminally consistent; clean and degraded paths meet their virtual-time budgets.

- [ ] **Step 6: Commit lifecycle shutdown**

```bash
git add crates/coding-agent-app
git commit -m "feat: quiesce and recover local runtime"
```

### Task 16: Build the Generated React Data Layer and SSE Reducer

**Files:**
- Create: `web/package.json`
- Create: `web/package-lock.json`
- Create: `web/tsconfig.json`
- Create: `web/tsconfig.app.json`
- Create: `web/tsconfig.node.json`
- Create: `web/vite.config.ts`
- Create: `web/vitest.config.ts`
- Create: `web/index.html`
- Create: `web/scripts/generate-api.mjs`
- Create: `web/src/vite-env.d.ts`
- Create: `web/src/api/generated/schema.d.ts`
- Create: `web/src/api/types.ts`
- Create: `web/src/api/client.ts`
- Create: `web/src/api/sse.ts`
- Create: `web/src/state/model.ts`
- Create: `web/src/state/reducer.ts`
- Create: `web/src/state/useAgentState.ts`
- Create: `web/src/test/setup.ts`
- Create: `web/src/api/client.test.ts`
- Create: `web/src/api/sse.test.ts`
- Create: `web/src/state/reducer.test.ts`
- Create: `web/src/state/useAgentState.test.tsx`

**Interfaces:**
- Produces: generated OpenAPI aliases, `ApiClient`, `SseClient`, normalized `AgentState`, pure `agentReducer`, and `useAgentState` orchestration.
- Invariant: no TypeScript file hand-writes a server DTO shape; aliases resolve through `components["schemas"]` from generated output.

- [ ] **Step 1: Pin the frontend toolchain and generate its lockfile**

Use Node 24 in `engines` and these exact package versions:

```json
{
  "name": "ngy-coding-agent-web",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "engines": {
    "node": ">=24"
  },
  "scripts": {
    "dev": "vite",
    "api:generate": "node scripts/generate-api.mjs",
    "api:check": "node scripts/generate-api.mjs --check",
    "typecheck": "tsc -b",
    "test": "vitest",
    "test:run": "vitest run",
    "build": "vite build",
    "e2e": "playwright test"
  },
  "dependencies": {
    "react": "19.2.7",
    "react-dom": "19.2.7"
  },
  "devDependencies": {
    "@playwright/test": "1.61.1",
    "@testing-library/jest-dom": "6.9.1",
    "@testing-library/react": "16.3.2",
    "@testing-library/user-event": "14.6.1",
    "@types/node": "24.13.3",
    "@types/react": "19.2.17",
    "@types/react-dom": "19.2.3",
    "@vitejs/plugin-react": "6.0.3",
    "jsdom": "29.1.1",
    "openapi-typescript": "7.13.0",
    "typescript": "5.9.3",
    "vite": "8.1.4",
    "vitest": "4.1.10"
  }
}
```

Create the first lockfile with `npm --prefix web install --package-lock-only`, then install only from it with `npm --prefix web ci`. Commit the lockfile; do not use a floating `npx` package.

Make `tsconfig.json` a build-mode reference to `tsconfig.app.json` and `tsconfig.node.json`. Both use `strict`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`, `isolatedModules`, `noEmit`, and bundler module resolution. The app config targets ES2023 with DOM/DOM.Iterable and `react-jsx`; the node config supplies Node 24 types for Vite/Vitest/Playwright configs. `tsc -b` must typecheck both.

- [ ] **Step 2: Export OpenAPI, write failing client/reducer tests, and verify red**

Run: `cargo run -p coding-agent-api --bin export_openapi -- web/openapi.json`

Run: `npm --prefix web run api:generate`

Write tests proving the URL fragment is cleared before exchange fetch begins, bootstrap normalizes repositories/tasks, duplicate event IDs are ignored, out-of-order/non-monotonic persisted IDs force a bootstrap, a current-schema event with an unknown named kind records a diagnostic, advances its validated persisted cursor, and continues, and an unsupported schema version forces a bootstrap. Test the streaming SSE parser across arbitrary UTF-8 chunk boundaries, CRLF/LF line endings, comment heartbeats, multi-line `data`, and an event name that was unknown when the client was built. With fake timers and injected jitter, prove reconnect delay is capped, reconnect carries `after=<lastAppliedId>`, `401` enters SessionExpired with no reconnect, clean EOF/`503` reconnect from the unchanged cursor, and malformed/oversized or EOF-truncated frames plus `stream.reset` bootstrap before reconnecting from the bootstrap cursor. If that recovery bootstrap fails, assert explicit unavailable/protocol state and capped bootstrap retry rather than a tight stream loop. Also prove a TaskDetail response replays only buffered IDs above its cursor, a slower earlier task-detail response cannot replace a later selection, older service generations cannot regress state, and cancel optimism rolls back on `503` or a competing terminal event.

Run: `npm --prefix web run test:run -- src/api/client.test.ts src/api/sse.test.ts src/state/reducer.test.ts src/state/useAgentState.test.tsx`

Expected: tests fail because client, reducer, and hook behavior is absent.

- [ ] **Step 3: Implement deterministic OpenAPI-to-TypeScript generation**

`generate-api.mjs` invokes the lockfile-installed `openapi-typescript` binary on `web/openapi.json`, writes a temporary sibling, and normalizes line endings to LF. Normal mode replaces `src/api/generated/schema.d.ts` only when bytes differ; `--check` compares the temporary bytes with the committed file and exits non-zero without modifying it. `types.ts` contains aliases such as:

```ts
import type { components } from "./generated/schema";

export type Task = components["schemas"]["TaskDto"];
export type TaskDetail = components["schemas"]["TaskDetailDto"];
export type TaskEvent = components["schemas"]["TaskEventDto"];
export type BootstrapResponse = components["schemas"]["BootstrapResponse"];
```

`api:check` performs that byte comparison, so it is cross-platform and leaves the worktree untouched.

- [ ] **Step 4: Implement session/bootstrap and typed REST commands**

On first load, parse `token` from `location.hash` into memory, synchronously call `history.replaceState` with path/query but no fragment, then and only then exchange it. A refresh with no fragment attempts authenticated bootstrap using the host-only cookie. `ApiClient` always uses same-origin relative URLs, `credentials: "same-origin"`, JSON decoding, and the bootstrap CSRF token for mutations. It surfaces stable code, message, retryable, request ID, and details.

Generate a new client request UUID once per user create action and reuse it on retry of an ambiguous network response. Never retry a mutation with a new ID. Treat `401` as SessionExpired: stop automatic mutation retries, close SSE, and tell the user to reopen from the native application. Treat `503 STORE_BUSY` as retryable UI feedback while retaining the same mutation request ID.

- [ ] **Step 5: Implement custom SSE reconnect and pure event projection**

`SseClient` uses same-origin `fetch` with cookies, `Accept: text/event-stream`, `redirect: "error"`, an `AbortController`, and a `ReadableStream` reader rather than native `EventSource`. Native `EventSource` has no catch-all for future named events, so it cannot implement the approved unknown-kind fallback. A small incremental parser uses streaming fatal-mode `TextDecoder`, accepts CRLF/LF/CR line endings, joins repeated `data:` lines with newlines, ignores comment heartbeats, preserves every named `event:` value, and dispatches only on a blank line; it handles arbitrary byte/chunk boundaries and caps buffered frame bytes. Every exit first closes/cancels the current reader, then follows exactly one recovery class: `401` enters SessionExpired and never reconnects; a clean EOF with no partial frame, transport failure, redirect rejection, `408`, `429`, and `5xx` retain the cursor and use capped-backoff reconnect; observable non-success responses other than those transient statuses, invalid content type, malformed UTF-8/JSON/envelopes, oversized or EOF-truncated frames, ID disagreement, and other protocol violations stop stream projection and require a full bootstrap before a new connection uses the bootstrap cursor. Fetch exposes a rejected redirect under `redirect: "error"` only as a network error, so the implementation deliberately does not claim to distinguish those two cases. A failed recovery bootstrap shows explicit unavailable/protocol state and retries bootstrap with capped backoff instead of reopening the same bad stream in a tight loop.

Reconnect uses capped exponential delay plus injected jitter and `after=<lastAppliedId>`. The client parses JSON as `unknown`, then narrows known variants through generated OpenAPI types. For a valid positive persisted ID and supported schema, an unknown named/data kind is recorded as an ignored diagnostic and advances the cursor without changing a panel, preventing endless replay; malformed envelopes, event-name/data-kind disagreement, non-monotonic persisted IDs, unsupported schema versions, and `stream.reset` trigger a full bootstrap. Named task events, `service.state`, and `stream.reset` all travel through this one parser, so future names are observable without pre-registration.

The pure reducer stores repositories/tasks by ID, task order separately, the selected task detail projection, applied global cursor, per-selected-task live buffer, service generation, and ephemeral command state. Snapshot variants replace plan/diff/tests; activity deduplicates stable entry IDs; lifecycle variants update task summary/timeline. A known schema with an unknown kind records a diagnostic and continues. Never hydrate repositories, tasks, panels, cursors, session data, or CSRF from localStorage; only harmless view preferences may persist.

- [ ] **Step 6: Implement `useAgentState` snapshot/live joining**

Start SSE immediately after bootstrap, without waiting for detail. Each selection increments a request generation, buffers its live events, and accepts the detail response only if its generation still matches. Install the snapshot, then replay sorted buffered events with `id > detail.event_cursor`; discard older/equal ones. Global task summaries keep updating for non-selected tasks. Re-selecting a task fetches a new detail rather than trusting an incomplete browser history.

- [ ] **Step 7: Run frontend data-layer gates and commit**

Run: `npm --prefix web run api:check`

Run: `npm --prefix web run typecheck`

Run: `npm --prefix web run test:run`

Expected: generation is clean and every client/reducer/hook race test passes.

```bash
git add web/package.json web/package-lock.json web/tsconfig.json web/tsconfig.app.json web/tsconfig.node.json web/vite.config.ts web/vitest.config.ts web/index.html web/openapi.json web/scripts web/src/api web/src/state web/src/test web/src/vite-env.d.ts
git commit -m "feat: add typed react data layer"
```

### Task 17: Implement the React Three-Pane Workbench

**Files:**
- Create: `web/src/components/AppShell.tsx`
- Create: `web/src/components/Sidebar.tsx`
- Create: `web/src/components/TaskWorkspace.tsx`
- Create: `web/src/components/TaskComposer.tsx`
- Create: `web/src/components/PlanPane.tsx`
- Create: `web/src/components/ActivityPane.tsx`
- Create: `web/src/components/ResultPane.tsx`
- Create: `web/src/components/ConnectionBanner.tsx`
- Create: `web/src/components/ErrorBoundary.tsx`
- Create: `web/src/components/AppShell.test.tsx`
- Create: `web/src/components/TaskWorkspace.test.tsx`
- Create: `web/src/styles.css`
- Create: `web/src/main.tsx`

**Interfaces:**
- Consumes: Task 16 hook and generated types only.
- Produces: desktop-first responsive three-pane workbench with repository/task navigation, task creation, cancel/retry, panel projections, and explicit app quit.
- Invariant: Project 1 never shows merge, review-pass, deliverable, or real-code-edit controls; Completed is labeled as fake execution completion only.

- [ ] **Step 1: Write failing interaction and accessibility tests**

Render the shell with a controllable hook adapter. Cover repository/task selection, empty states, create validation, the action matrix for Queued, Running, Completed, Failed, Cancelled, and Interrupted, cancellation-in-progress disabling, retry creation followed by linear retry-chain navigation, read-only controls on older attempts, quit confirmation, service banners, degraded-shutdown warning, slow/error panels, and request-ID display. Use role/name queries and keyboard user events.

```tsx
it("keeps cancel pending local and yields to a terminal server event", async () => {
  const fixture = renderRunningTask();
  await userEvent.click(screen.getByRole("button", { name: "Cancel task" }));
  expect(screen.getByText("Cancelling")).toBeVisible();
  fixture.emitCompleted();
  expect(screen.getByText("Execution completed — not reviewed")).toBeVisible();
  expect(screen.queryByText("Cancelling")).not.toBeInTheDocument();
});
```

Add automated assertions for landmarks, visible focus, labels, `aria-live="polite"`, and text/icon status cues independent of color.

- [ ] **Step 2: Run component tests and verify red**

Run: `npm --prefix web run test:run -- src/components/AppShell.test.tsx src/components/TaskWorkspace.test.tsx`

Expected: tests fail because the workbench components do not exist.

- [ ] **Step 3: Implement shell, sidebar, composer, and connection states**

Use semantic header/nav/main/aside regions with visible titles. The left pane lists repositories by last opened time and tasks by creation time, exposes direct path registration plus native picker, and offers Retry for eligible interrupted/terminal tasks while keeping old attempts selectable/read-only. It preserves only harmless selection/collapse preference. The composer trims prompt, shows scalar count against 50,000, holds one stable client request ID during an ambiguous retry, and associates API errors with its fields.

The header renders Connected, Reconnecting, Store degraded, Shutting down, Session expired, or Server unavailable. The app menu's explicit quit action is separate from browser unload; do not register `beforeunload` cancellation or shutdown handlers.

- [ ] **Step 4: Implement center and result panes with isolated error boundaries**

The center pane shows task title/prompt/attempt/status, three-step plan snapshot, activity, composer, and Running cancel. The right pane shows synthetic diff, test snapshots, lifecycle timeline with structured failure, and the linear retry chain with read-only navigation to older attempts. Give all evidence areas textual statuses and empty/loading/error states. Wrap plan/activity/diff/tests/timeline areas independently so a rendering failure leaves navigation and cancel usable. New activity uses one non-interruptive live region and never moves focus.

For Running cancellation, show local Cancelling only until a REST snapshot/event resolves it or an error rolls it back. Old attempts are read-only except Retry on eligible terminal states. Never infer review or merge readiness from Completed.

- [ ] **Step 5: Implement responsive layout and visual states**

Use CSS Grid for three desktop columns with bounded resizable-looking surfaces but no persisted domain layout. At narrower widths, retain all three semantic regions in a stacked/tabbed presentation. Define high-contrast focus rings, reduced-motion support, non-color status glyph/text, scroll containment, and readable diff wrapping. Load no remote fonts, images, scripts, or styles.

- [ ] **Step 6: Run UI and full frontend gates**

Run: `npm --prefix web run typecheck`

Run: `npm --prefix web run test:run`

Run: `npm --prefix web run build`

Expected: component/data tests pass and Vite emits `web/dist` without external asset URLs.

- [ ] **Step 7: Commit the workbench**

```bash
git add web/src/components web/src/styles.css web/src/main.tsx
git commit -m "feat: add react coding workbench"
```

### Task 18: Embed the Production Web Build and Enforce Browser Policies

**Files:**
- Modify: `crates/coding-agent-app/Cargo.toml`
- Create: `crates/coding-agent-app/build.rs`
- Create: `crates/coding-agent-app/src/static_assets.rs`
- Create: `crates/coding-agent-app/tests/static_assets.rs`
- Modify: `crates/coding-agent-app/src/server.rs`
- Modify: `crates/coding-agent-app/src/main.rs`
- Modify: `web/vite.config.ts`
- Modify: `.gitignore`

**Interfaces:**
- Produces: Cargo features `embedded-web` and `e2e`, `StaticAssetService`, SPA fallback, deterministic cache/security headers, and a release-build guard.
- Invariant: a release binary cannot compile without embedded assets; at runtime it needs neither Node nor `web/dist`.

- [ ] **Step 1: Write failing static-asset and header tests**

Build a minimal Vite fixture and test `/`, one hashed JS asset, an unknown SPA route, an unknown filename with an extension, and `/api/not-a-route`. Assert MIME types, exact body bytes, `no-store` for HTML/API, one-year immutable caching for hashed assets, 404 for missing assets/API, and index fallback only for extensionless non-API GET requests accepting HTML.

Assert every production response has `X-Content-Type-Options: nosniff` and `Referrer-Policy: no-referrer`; HTML has the exact approved CSP with only self/data sources and no inline allowance. Assert no CORS header exists.

- [ ] **Step 2: Run the embedded-asset test and verify red**

Run: `npm --prefix web run build`

Run: `cargo test -p coding-agent-app --test static_assets --features embedded-web`

Expected: compilation or tests fail because the embedding service/feature is absent.

- [ ] **Step 3: Define build features and release guard**

Declare optional `rust-embed` and these feature edges:

```toml
[dependencies]
mime_guess.workspace = true
rust-embed = { workspace = true, optional = true }

[features]
default = []
embedded-web = ["dep:rust-embed"]
test-support = []
e2e = ["embedded-web", "rust-embed/debug-embed", "test-support"]
```

`build.rs` emits `rerun-if-changed` for `../../web/dist`. `main.rs` uses `compile_error!` under `all(not(debug_assertions), not(feature = "embedded-web"))`, so an unusable release build is impossible. Debug development without `embedded-web` expects the explicit Vite proxy; E2E uses `e2e` so debug assets are truly embedded.

- [ ] **Step 4: Implement embedded lookup and safe SPA fallback**

Derive `RustEmbed` over `web/dist`, normalize request paths without accepting backslashes, dot segments, percent-decoded traversal, or NUL, and look up exact assets first. Serve `index.html` only for GET/HEAD outside `/api` and `/_local`, without a filename extension, and with HTML accepted. Use `mime_guess`; HEAD returns identical headers and an empty body.

Set Vite `build.outDir = "dist"`, `emptyOutDir = true`, and `manifest = true`. Detect content-hashed filenames from the embedded `.vite/manifest.json` rather than a permissive regex, and never serve that manifest or any dot-prefixed internal path. Hashed assets get `public,max-age=31536000,immutable`; HTML and every API response get `no-store`. The outer server layer sets CSP, nosniff, and referrer policy on success and error responses.

- [ ] **Step 5: Verify dev proxy and production contract order**

Vite proxies only `/api` and `/_local` to an explicitly provided Axum target and preserves SSE streaming. The backend development `public_origin` is the one configured Vite origin; no wildcard localhost rule exists.

Run in this order:

```bash
cargo run -p coding-agent-api --bin export_openapi -- web/openapi.json
npm --prefix web ci
npm --prefix web run api:check
npm --prefix web run typecheck
npm --prefix web run test:run
npm --prefix web run build
cargo test -p coding-agent-app --test static_assets --features embedded-web
cargo build --release -p coding-agent-app --features embedded-web
```

Expected: all commands pass, embedded-service tests serve the built bytes, and the guarded release executable is produced. Task 20's real-process release smoke owns the stronger proof that this artifact serves `/` from a clean directory with `web/dist` and Node unavailable; do not claim that runtime result from build-only evidence here.

- [ ] **Step 6: Commit production embedding**

```bash
git add .gitignore crates/coding-agent-app web/vite.config.ts Cargo.lock
git commit -m "feat: embed secured react application"
```

### Task 19: Add Real-Process Playwright Coverage and Fault Injection

**Files:**
- Create: `crates/coding-agent-app/src/test_support.rs`
- Create: `crates/coding-agent-app/tests/process_support.rs`
- Create: `web/playwright.config.ts`
- Create: `web/e2e/support/localApp.ts`
- Create: `web/e2e/local-app.spec.ts`
- Modify: `crates/coding-agent-app/src/main.rs`
- Modify: `crates/coding-agent-app/src/store_writer.rs`
- Modify: `crates/coding-agent-app/src/fake_runner.rs`

**Interfaces:**
- Produces: feature-gated process configuration, deterministic fake/store fault scripts, and a Playwright harness for a real Axum/SQLite/SSE/React process.
- Invariant: production builds have no test HTTP routes, prompt magic strings, data-dir override, scenario parser, or fault injector.

- [ ] **Step 1: Write a failing process-support contract and draft the session E2E**

Under `#[cfg(feature = "test-support")]`, write `tests/process_support.rs` against `ProcessTestConfig::load(path)`. Use private temporary app-data/runtime roots and a scenario file to assert the closed schema accepts the complete approved fixture, rejects an unknown field, validates all paths before actor startup, consumes the file exactly once, and leaves no scenario bytes at the source path after successful load.

Run: `cargo test -p coding-agent-app --features test-support --test process_support`

Expected: compilation fails because the feature-gated `test_support` module and `ProcessTestConfig` do not exist. Do not launch the current binary for this red step: before the override is implemented it would use real user application paths.

`localApp.ts` must create a private temporary app-data/runtime root and a real temporary Git/Cargo repository, spawn the binary named by `CODING_AGENT_E2E_BINARY`, wait for the atomic descriptor, call launcher-protected `/_local/reopen`, navigate to its fragment URL, and guarantee child cleanup. It records stdout/stderr only on test failure and redacts descriptor secrets.

Draft the first Playwright test to assert the fragment is absent from `location.href` and browser history before the exchange request is observed, bootstrap succeeds, no CORS header is present, and every page request is same-origin loopback or a `data:` URL. Its first execution happens only in Step 3, after process isolation exists and an explicit binary path has been exported.

- [ ] **Step 2: Implement compile-time-isolated test support**

Under Cargo feature `test-support` only, accept environment paths for app data, runtime descriptor, and one JSON scenario file loaded before actors start. The closed schema contains ordered `FakeScenario` values, StoreWriter fault points/counts, actor pause points, virtual release signals, and marker-write failure. Reject unknown fields and delete/zero the parsed bytes after construction.

Production `FakeTaskRunner` remains Success-only. Production StoreWriter has no fault branch. Expose no HTTP route for changing a scenario; the only browser-visible surface is the ordinary product API.

Run: `cargo test -p coding-agent-app --features test-support --test process_support`

Expected: the closed-schema, single-consumption, validation, and source-byte-removal tests pass.

- [ ] **Step 3: Complete the real-process harness**

Build with `cargo build -p coding-agent-app --features e2e` after `web/dist` exists. The harness writes scenario JSON, starts one process with the feature-gated environment, reads the descriptor with bounded reopen retries, and uses the returned URL. Helpers create tasks through the visible UI, poll accessible statuses, start a second binary, kill/restart the primary with the same database, and inspect only authorized API responses.

Run Playwright with one worker for lifecycle scenarios. Do not use `page.route` to mock product API/SSE; interception may only fail the test on a non-loopback outbound request.

Run: `npm --prefix web run build`

Run: `cargo build -p coding-agent-app --features e2e`

Run: `npm --prefix web exec -- playwright install chromium`

PowerShell: `$env:CODING_AGENT_E2E_BINARY=(Resolve-Path target/debug/coding-agent-app.exe)`

PowerShell: `npm --prefix web run e2e -- --grep "clears launch token"`

POSIX: `export CODING_AGENT_E2E_BINARY="$PWD/target/debug/coding-agent-app"`

POSIX: `npm --prefix web run e2e -- --grep "clears launch token"`

Expected: the first real-process test passes using only the isolated paths and explicit binary.

- [ ] **Step 4: Add core workflow and concurrency E2E scenarios**

Cover adding, reusing, and switching two real Git/Cargo repositories; compare each fixture's file list, lockfile bytes, and dirty status before/after discovery. Then cover task creation, deterministic plan/activity/diff/tests, refresh and full page close/reopen while tasks continue, 4 Running plus fifth/sixth Queued, cancelling the fifth before releasing a permit so only the sixth starts, Running cancel, first-commit-wins cancel/completion, failure, panic isolation, retry idempotency, old attempt read-only behavior, and UI error/request-ID rendering. Run the claim/cancel scenario separately at permit acquired, provisional handle registered, and Running committed pauses; none may expose Running without a cancellation token.

Add a direct unauthorized-request matrix for missing/wrong cookie, Host, Origin, CSRF, launcher secret, and launch-token replay. Verify native picker cannot be opened unauthenticated through its HTTP endpoint.

- [ ] **Step 5: Add lifecycle, recovery, and fault E2E scenarios**

Cover a second process reopening the first without a second writer, forced kill at commit-before-wake, recovery-before-descriptor, and descriptor-before-browser points, restart conversion of Queued/Running to Interrupted, lost writer/dispatcher notifications, receiver lag catch-up, and stream.reset. For recoverable background terminal-write failure, assert the UI shows Store degraded, no new claim starts, recovery changes the ambiguous task to Interrupted, and the banner returns to Connected only after the persisted event is visible. During Web UI quit, release concurrent create/retry/claim/result pauses around the barrier and assert no late Queued/Running commit survives and unfinished tasks are Interrupted rather than Cancelled.

For a runner that ignores cancellation, invoke Web UI quit and assert its Task is durably Interrupted and the process does not wait beyond 10 seconds. For permanent Store failure, invoke quit from the Web UI and assert non-zero exit within 10 seconds. In one variant assert the private marker remains and, after restoring writes, the next startup interrupts incomplete tasks and removes it; in a second variant force marker creation to fail and still assert timely lock/descriptor release.

For TaskDetail/SSE joining, pause detail after its read snapshot, commit a live event, then release and assert the panel contains it exactly once. For service state, change generation between bootstrap and SSE and assert the first control prevents regression.

- [ ] **Step 6: Build and run the full real-process suite**

Run: `npm --prefix web run build`

Run: `cargo build -p coding-agent-app --features e2e`

PowerShell: `$env:CODING_AGENT_E2E_BINARY=(Resolve-Path target/debug/coding-agent-app.exe)`

PowerShell: `npm --prefix web run e2e`

POSIX: `export CODING_AGENT_E2E_BINARY="$PWD/target/debug/coding-agent-app"`

POSIX: `npm --prefix web run e2e`

Expected: all tests drive the real embedded application; no mocked API call or non-loopback request occurs.

- [ ] **Step 7: Commit process-level verification**

```bash
git add crates/coding-agent-app web/playwright.config.ts web/e2e
git commit -m "test: cover local app as a real process"
```

### Task 20: Add Three-OS CI, Release Smoke, and Operator Documentation

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `crates/coding-agent-app/tests/release_smoke.rs`
- Create: `scripts/check-placeholders.mjs`
- Create: `README.md`
- Modify: `Cargo.lock`
- Modify: `web/package-lock.json`

**Interfaces:**
- Produces: one Linux full-quality/E2E job, a Windows/macOS/Linux release-smoke matrix, and exact development/release runbooks.
- Invariant: the tested release artifact runs from a clean directory with Node and `web/dist` unavailable to its child process.

- [ ] **Step 1: Write a failing cross-platform release smoke**

`release_smoke.rs` reads `CODING_AGENT_RELEASE_BINARY`, copies only that executable into a clean temporary directory, removes Node-containing entries from the child's `PATH`, redirects OS application-data/runtime environment variables into the temporary tree, and asserts `node --version` cannot spawn in that same child environment. It starts the production artifact with no CLI arguments, waits for the private descriptor, asserts its port is a random `127.0.0.1` listener, uses raw HTTP/1.1 to verify launcher-protected readiness, obtains a one-time URL, exchanges it, fetches bootstrap and `/`, then sends protected quit and asserts clean exit plus descriptor removal.

Build a default-feature debug application in a dedicated target directory so Task 19's embedded E2E artifact cannot be reused accidentally. Export its absolute path, then run the ignored smoke against that known non-embedded binary:

```powershell
$redTarget = [System.IO.Path]::GetFullPath('target/release-smoke-red-app')
$env:CARGO_TARGET_DIR = $redTarget
cargo build -p coding-agent-app
Remove-Item Env:CARGO_TARGET_DIR
$env:CODING_AGENT_RELEASE_BINARY = Join-Path $redTarget 'debug/coding-agent-app.exe'
cargo test -p coding-agent-app --test release_smoke -- --ignored --exact release_binary_starts_without_node_or_dist
```

```bash
CARGO_TARGET_DIR="$PWD/target/release-smoke-red-app" cargo build -p coding-agent-app
export CODING_AGENT_RELEASE_BINARY="$PWD/target/release-smoke-red-app/debug/coding-agent-app"
cargo test -p coding-agent-app --test release_smoke -- --ignored --exact release_binary_starts_without_node_or_dist
```

Expected: the test reaches its production-artifact startup/static assertion and fails because this explicit binary has no embedded web assets. Missing environment variables, a missing executable, or reuse of an `e2e` binary is not the intended red result.

- [ ] **Step 2: Implement the release smoke without a second runtime dependency**

Use only Rust standard-library TCP/process/file APIs plus existing serde types; do not add a Node, browser, curl, Python, or TLS client dependency. Preserve OS system directories in PATH while filtering Node directories so browser launch can use the platform mechanism. The smoke accepts a browser-open attempt but never assumes the browser page participates. All temporary data is removed after the child exits.

After `npm --prefix web run build`, run:

```bash
cargo build --release -p coding-agent-app --features embedded-web --locked
```

PowerShell: `$env:CODING_AGENT_RELEASE_BINARY=(Resolve-Path target/release/coding-agent-app.exe)`

POSIX: `export CODING_AGENT_RELEASE_BINARY="$PWD/target/release/coding-agent-app"`

Then run:

```bash
cargo test -p coding-agent-app --test release_smoke --features embedded-web -- --ignored --exact release_binary_starts_without_node_or_dist
```

- [ ] **Step 3: Add deterministic Linux quality and E2E CI**

The Linux job uses Node 24 and `rust-toolchain.toml`, installs dependencies/browsers first, then exports `CARGO_NET_OFFLINE=true` and `npm_config_offline=true` for all build/test gates. Run this exact order:

```bash
cargo fetch --locked
npm --prefix web ci
npm --prefix web exec -- playwright install --with-deps chromium
export CARGO_NET_OFFLINE=true
export npm_config_offline=true
cargo run --locked --offline -p coding-agent-api --bin export_openapi -- web/openapi.json
npm --prefix web run api:check
git diff --exit-code -- web/openapi.json web/src/api/generated/schema.d.ts
npm --prefix web run typecheck
npm --prefix web run test:run
npm --prefix web run build
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo test --workspace --all-features --locked --offline
cargo build --locked --offline -p coding-agent-app --features e2e
export CODING_AGENT_E2E_BINARY="$PWD/target/debug/coding-agent-app"
npm --prefix web run e2e
cargo build --release --locked --offline -p coding-agent-app --features embedded-web
node scripts/check-placeholders.mjs
```

Export the E2E binary path explicitly. After OpenAPI/TypeScript generation, fail on any tracked diff. Playwright also fails if the page attempts a non-loopback network request.

- [ ] **Step 4: Add the Windows/macOS/Linux release-smoke matrix**

Each matrix leg runs `npm --prefix web ci`, API generation check, typecheck, frontend tests/build, Rust fmt/clippy/tests for supported platform targets, production release build, and `release_smoke.rs`. Full Chromium E2E runs on Linux; the other two platforms still exercise real listener, descriptor permissions, embedded HTML, session/bootstrap, and clean quit.

The matrix must catch Unix case-sensitive repository identity, Windows case-insensitive identity/DACL behavior, macOS native-dialog adapter construction, lock/descriptor atomicity, and an artifact started outside the repository. Do not mark any platform smoke `continue-on-error`.

- [ ] **Step 5: Document direct launch, development, security, and scope**

README covers prerequisites, Vite-plus-Axum development with an explicit public origin, production build order, direct executable launch, app-data/runtime locations, how to exit from the Web UI, browser-open failure recovery, database backup, and stable troubleshooting codes. State plainly that Project 1 is a deterministic fake platform: it does not read/modify source, call a model, create worktrees, run repository tests, review, merge, or imply deliverability. Explicitly defer installers, macOS app bundles, Linux desktop entries, signing/notarization, auto-update, and polished launcher packaging to Project 4; none is a Project 1 CI gate.

Document the threat boundary: loopback + Host/Origin/CSRF protects against ordinary cross-site access, not a malicious process already running as the same OS user. Document that closing the browser does not stop tasks or the app.

- [ ] **Step 6: Run the final verification sequence with only Task 20 edits present**

Run every Task 20 CI command locally where the platform supports it, plus:

```bash
git diff --check
node scripts/check-placeholders.mjs
git status --short
```

`check-placeholders.mjs` obtains tracked plus untracked non-ignored paths from `git ls-files --cached --others --exclude-standard`, excludes implementation-plan markdown and its own marker-definition source, scans the remaining text files for forbidden markers, and exits zero only with no matches. Expected: format, lint, all Rust/frontend/E2E tests, OpenAPI drift, embedded release, and release smoke are green; `git diff --check` and the marker scan are clean; status lists only the Task 20 files and any lockfile deltas named above.

- [ ] **Step 7: Commit CI and release documentation**

```bash
git add .github/workflows/ci.yml crates/coding-agent-app/tests/release_smoke.rs scripts/check-placeholders.mjs README.md Cargo.lock web/package-lock.json
git commit -m "build: verify local app releases"
```

- [ ] **Step 8: Verify the committed release gate is clean**

Run: `git status --short`

Expected: no output. If any generated or lockfile change remains, rerun its owning gate, add it to the Task 20 commit, and repeat the full affected checks before claiming completion.
