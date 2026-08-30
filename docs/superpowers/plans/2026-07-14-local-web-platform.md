# 本地 Web 平台实施计划

> 状态：历史实施计划；Project 1 已完成并验收。下列未勾选框保留原始执行模板，不表示当前欠项。
> **历史执行说明：** 当时要求使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans` 按任务实施，并以复选框（`- [ ]`）跟踪执行过程。

> 2026-08-29 范围修订：本历史计划中“安装/签名/启动器打包推迟到 Project 4”的措辞按未来 P4-D 解释；后续批准的 Project 4 仅为 P4-A + P4-B。原 TDD 步骤与当时验收事实保持不变。

**目标：** 将 Project 1 构建为可直接启动的本地 Rust 应用程序，具备受保护的 React 三窗格界面、确定性假任务、SQLite 持久化、REST 命令、可重放 SSE、重启恢复及跨平台冒烟测试覆盖。

**架构：** 四个 Rust 软件包保持已批准的依赖方向：`domain` 负责纯规则，`store` 负责 SQLite，`api` 负责 REST/SSE 契约，`app` 组合执行体与原生平台服务。React 使用由 OpenAPI 生成的 TypeScript 类型，将 REST 快照视为权威数据，并将持久化的 SSE 事件作为幂等增量应用。Project 1 仅使用 `FakeTaskRunner`；真实模型、代码工具、Git 工作树、角色和合并行为不在本计划范围内。

**技术栈：** Rust 1.97.0、edition 2024、Axum 0.8.9、Tokio 1.52.3、采用 SQLite 的 SQLx 0.9.0、Utoipa 5.5.0、React 19.2.7、TypeScript 5.9.3、Vite 8.1.4、Vitest 4.1.10、Playwright 1.61.1。

## 全局约束

- 在使用 `superpowers:using-git-worktrees` 创建的隔离 Git 工作树中执行本计划；不得直接在 `main` 上实施。
- Project 1 只能检查 Git/Cargo 注册元数据。不得读取源文件、调用模型、创建 Git 工作树、修改仓库或运行仓库测试。
- 生产环境假运行器的并发数严格为 4。在后续 P4-A 之前，未来真实运行器的默认并发数为 1；不得将假任务并发数编码进 API 类型。
- `TaskStatus::Completed` 只表示运行器成功。绝不表示已审查、可交付或可合并。
- SQLite 是权威数据源。每个可观察的任务状态变更及其事件必须在同一事务中提交；内存通道仅用于加速。
- 任务领取/取消、运行器事件/结果、状态协调和静默化由 `TaskManager` 串行化；运行时执行体启动后，创建/重试以及每次 SQLite 变更由 `StoreWriter` 串行化；迁移/启动恢复更早在唯一主实例的独占控制下运行。每个持久化实时事件由 `EventDispatcher` 按 ID 顺序发布。
- 仅在随机端口上绑定 `127.0.0.1`。生产环境不启用 CORS，也不使用远程/CDN 资源。
- 所有 `/api/*` 读取都需要进程会话。每个变更操作还需要完全匹配的 `Origin` 和 `X-CSRF-Token`。
- 刷新或关闭浏览器绝不取消工作。启动时将持久化的 `Queued`/`Running` 任务转换为 `Interrupted`；绝不自动恢复它们。
- Rust OpenAPI 是 API DTO 的唯一来源。前端别名可以引用生成的类型，但不得重复定义 DTO 结构。
- 所有持久化及 API 时间戳均经过 `UtcTimestamp`，并序列化为 UTC RFC 3339；仓库路径必须先经过 `CanonicalPath`，再持久化或映射到响应。
- 发布顺序为：导出 OpenAPI → `npm ci` → 检查 TypeScript 生成结果 → 类型检查/测试/构建 → Rust 检查/测试 → 发布构建。
- 安装依赖时可以使用注册表；执行测试和应用程序正常运行时不得发起任何外部网络请求。
- 每项行为都采用 TDD：先观察聚焦测试因预期原因失败，再实施最小行为，观察其通过，运行受影响的测试套件，随后提交。
- 不要为 Project 2 添加兼容层。仅保留已批准的 `TaskRunner` 和带版本的事件接缝。

## 来源规范

- `docs/superpowers/specs/2026-07-14-coding-agent-product-roadmap-design.md`
- `docs/superpowers/specs/2026-07-14-local-web-platform-design.md`

## 锁定文件映射

```text
Cargo.toml                                  Rust 工作区和锁定的共享依赖
Cargo.lock                                  已提交的 Rust 依赖锁文件
rust-toolchain.toml                         Rust 1.97.0 工具链版本固定
crates/coding-agent-domain/
  Cargo.toml
  src/lib.rs                                公开的领域层导出
  src/ids.rs                                UUID 新类型
  src/value.rs                              经验证的路径/时间/事件游标值
  src/repository.rs                         仓库及注册输入
  src/task.rs                               任务状态机和失败信息
  src/event.rs                              事件种类和面板快照
  tests/state_machine.rs                    合法转换和重试测试
crates/coding-agent-store/
  Cargo.toml
  migrations/0001_initial.sql               repositories/tasks/events/schema_migrations
  src/lib.rs                                Store 入口点和读取池
  src/migrate.rs                            内嵌单调迁移执行器
  src/repositories.rs                       仓库身份和更新或插入
  src/tasks.rs                              任务/事件事务和恢复
  src/projection.rs                         BootstrapSnapshot 和 TaskDetail 重放
  tests/migrations.rs
  tests/repositories.rs
  tests/tasks.rs
  tests/projection.rs
  tests/support/mod.rs                    各测试目标导入的共享 SQLite 测试夹具
crates/coding-agent-api/
  Cargo.toml
  src/lib.rs
  src/contract.rs                           REST/SSE/OpenAPI DTOs
  src/backend.rs                            ApiBackend 和 RequestSecurity 端口
  src/error.rs                              ApiErrorResponse 映射
  src/router.rs                             受保护的 REST 路由处理器
  src/sse.rs                                SSE 汇合和传输帧
  src/bin/export_openapi.rs                 确定性 OpenAPI 导出器
  tests/openapi.rs
  tests/router.rs
  tests/sse.rs
  tests/support/mod.rs                    假 API/安全/SSE 端口
crates/coding-agent-app/
  Cargo.toml
  build.rs                                  前端内嵌重建触发器
  src/lib.rs
  src/main.rs                               主/辅助实例组合根
  src/service_state.rs                      就绪/降级/静默化状态世代号
  src/store_writer.rs                       单一 SQLite 变更执行体
  src/event_dispatcher.rs                   由数据库支持的有序实时发布器
  src/task_manager.rs                       单一任务控制执行体
  src/fake_runner.rs                        确定性运行器和测试脚本
  src/repository_service.rs                 只读 Git/Cargo 发现
  src/native_dialog.rs                      串行化的选择器/消息对话框端口
  src/security.rs                           会话、令牌、Host、Origin、CSRF
  src/platform.rs                           应用路径、私有权限、浏览器
  src/single_instance.rs                    文件锁和运行时描述符
  src/server.rs                             外层 Axum 路由器和就绪状态
  src/static_assets.rs                      开发环境后备和发布内嵌
  src/shutdown.rs                           正常和降级关闭
  src/test_support.rs                       受功能特性限制的进程测试注入
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
  tests/support/mod.rs                    各测试目标导入的共享执行体/平台测试夹具
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
.github/workflows/ci.yml                  Rust、前端、E2E 和三个操作系统的冒烟门禁
scripts/check-placeholders.mjs             已跟踪源文件的禁用标记门禁
README.md                                 直接启动和开发工作流
```

---

### 任务 1：建立工作区和纯领域模型

**文件：**
- 创建：`Cargo.toml`
- 创建：`rust-toolchain.toml`
- 创建：`crates/coding-agent-domain/Cargo.toml`
- 创建：`crates/coding-agent-domain/src/lib.rs`
- 创建：`crates/coding-agent-domain/src/ids.rs`
- 创建：`crates/coding-agent-domain/src/value.rs`
- 创建：`crates/coding-agent-domain/src/repository.rs`
- 创建：`crates/coding-agent-domain/src/task.rs`
- 创建：`crates/coding-agent-domain/src/event.rs`
- 创建：`crates/coding-agent-domain/tests/state_machine.rs`
- 创建：`crates/coding-agent-store/Cargo.toml`
- 创建：`crates/coding-agent-store/src/lib.rs`
- 创建：`crates/coding-agent-api/Cargo.toml`
- 创建：`crates/coding-agent-api/src/lib.rs`
- 创建：`crates/coding-agent-app/Cargo.toml`
- 创建：`crates/coding-agent-app/src/lib.rs`
- 修改：`.gitignore`

**接口：**
- 产出：`RepositoryId`、`TaskId`、`ClientRequestId`、`CanonicalPath`、`UtcTimestamp`、`EventId`、`EventCursor`、`DomainError`、`Repository`、`NewRepository`、`Task`、`NewTask`、`TaskStatus`、`TaskFailure`、`TaskEvent`、`TaskEventKind`、`TaskEventPayload`、`PlanSnapshot`、`ActivityEntry`、`DiffSnapshot`、`TestSnapshot` 和 `TimelineEntry`。
- 不变量：`TaskStatus::can_transition_to` 是唯一的合法转换表；存储层代码不得复制该表。

- [ ] **步骤 1：创建工作区清单和一个失败的状态机测试**

使用以下工作区依赖集，并在 Cargo 生成 `Cargo.lock` 时将其提交；独立的 npm 锁文件将在任务 16 中创建：

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

现在创建全部四个成员清单，使 Cargo 能触发预期的领域层编译失败。三个尚未实施的 `src/lib.rs` 文件只包含一条软件包级职责注释。后续任务会在代码首次使用运行时依赖时再添加它们。

将 `/target`、`/web/node_modules`、`/web/dist` 和 Playwright 输出目录添加到 `.gitignore`；继续跟踪 `Cargo.lock`、`web/package-lock.json`、`web/openapi.json` 以及生成的 TypeScript 声明。

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

- [ ] **步骤 2：运行聚焦测试并确认预期失败**

运行：`cargo test -p coding-agent-domain --test state_machine`

预期：由于 `coding_agent_domain::TaskStatus` 尚不存在，编译失败。工具链或依赖下载失败并不是预期的红灯结果；修复环境并重新运行，直到缺失类型成为失败原因。

- [ ] **步骤 3：实施领域类型和唯一转换表**

使用透明的 UUID 新类型（newtype），并确保该软件包中不出现 HTTP/OpenAPI 名称：

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

其余领域结构在此固定，并由存储层/API 映射原样复用：

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

所有结构体/枚举均派生适当的 `Debug`/`Clone`/`Eq`/`serde` 特征；状态枚举使用 `snake_case`。`RepositoryId`、`TaskId` 和 `ClientRequestId` 是彼此不同的透明 `uuid::Uuid` 新类型，带有 `new()` 以及 `Display`/`FromStr`。`NewTask::try_new` 会去除提示词两端空白，通过 `DomainError::InvalidPrompt` 拒绝空提示词或超过 50,000 个 Unicode 标量值的提示词，并存储去除空白后的值。`Store` 构造函数强制要求 `attempt >= 1` 且 `last_event_id > 0`。`Queued` 没有时间戳/失败信息；`Running` 只有开始时间；`Completed` 有开始/结束时间且无失败信息；`Failed` 有开始/结束时间和失败信息；`Cancelled` 与 `Interrupted` 有结束时间以及可选的开始时间，且只有 `Interrupted` 必须提供失败信息。

在 `ids.rs` 中定义 ID，在 `repository.rs` 中定义仓库类型，并在 `event.rs` 中定义以下严格一致的事件变体：

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

`CanonicalPath::try_from_canonical` 只接受绝对、规范化且不含当前/父级组件的路径；平台发现功能在调用它之前执行文件系统规范化。`UtcTimestamp` 会规范为 `UtcOffset::UTC`，解析 RFC 3339，并始终序列化为固定宽度的 UTC 形式 `YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ`，从而使 SQLite 文本顺序等同于时间顺序。它绝不暴露非 UTC 值。`EventId::new` 只接受正值；`EventCursor::new` 接受非负值并提供 `ZERO`。这四种类型均为字段私有的 serde 新类型，带有受检查的构造函数/访问器；事件 ID/游标值派生 `Copy`/`Ord`，并在 SQL/API 边界进行显式转换。

`TaskEventKind` 穷举映射这十个变体，`TaskEventPayload::kind()` 是唯一映射。`TaskEvent::new` 将模式版本固定为 1。扩展 `state_machine.rs`，加入空值、50,000 和 50,001 个标量值的提示词测试；UUID 和事件 ID/游标往返测试；规范路径拒绝测试；非 UTC 时间戳规范化/RFC3339 输出测试；`Task` 不变量测试；以及带标签事件序列化测试。任何类型都不得包含审查/交付字段。

- [ ] **步骤 4：运行领域测试并确认通过**

运行：`cargo test -p coding-agent-domain`

预期：状态机、提示词边界、ID、不变量和带标签事件测试全部通过，零失败。

- [ ] **步骤 5：对新软件包运行格式和代码检查**

运行：`cargo fmt --all --check`

运行：`cargo clippy -p coding-agent-domain --all-targets -- -D warnings`

预期：两个命令均以 0 退出，且无诊断信息。

- [ ] **步骤 6：提交可独立测试的领域基础**

```bash
git add Cargo.toml Cargo.lock rust-toolchain.toml .gitignore crates/coding-agent-domain crates/coding-agent-store/Cargo.toml crates/coding-agent-store/src/lib.rs crates/coding-agent-api/Cargo.toml crates/coding-agent-api/src/lib.rs crates/coding-agent-app/Cargo.toml crates/coding-agent-app/src/lib.rs
git commit -m "feat: add project domain model"
```

### 任务 2：添加 SQLite 迁移和仓库注册

**文件：**
- 修改：`crates/coding-agent-store/Cargo.toml`
- 创建：`crates/coding-agent-store/migrations/0001_initial.sql`
- 修改：`crates/coding-agent-store/src/lib.rs`
- 创建：`crates/coding-agent-store/src/migrate.rs`
- 创建：`crates/coding-agent-store/src/repositories.rs`
- 创建：`crates/coding-agent-store/tests/migrations.rs`
- 创建：`crates/coding-agent-store/tests/repositories.rs`
- 创建：`crates/coding-agent-store/tests/support/mod.rs`

**接口：**
- 使用：领域层（domain）中的 `Repository`、`RepositoryId` 和 `NewRepository`。
- 产出：`Store::open`、`Store::migrate`、`Store::register_repository`、`RegisterRepositoryOutcome::{Created, Existing}`，以及按 `(last_opened_at DESC,id)` 排序、不分页的只读 `Store::list_repositories`。
- 不变量：显示路径绝不用于定义身份；`(git_identity_key, cargo_identity_key)` 是唯一的。

本任务的存储层清单增量必须严格如下：

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

- [ ] **步骤 1：编写失败的迁移和仓库幂等性测试**

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

`tests/support/mod.rs` 负责 `memory_store`、基于文件的临时 `Store`、仓库构建器和数据库故障辅助函数；每个存储层集成测试都以 `mod support;` 开头。`migrations.rs` 必须断言 `PRAGMA journal_mode`、`PRAGMA foreign_keys`、非零忙等待超时、第二次迁移的幂等性，以及 `schema_migrations`、`repositories`、`tasks` 和 `task_events` 的存在。

- [ ] **步骤 2：运行存储层测试并验证红灯结果**

运行：`cargo test -p coding-agent-store --test migrations --test repositories`

预期：由于 `Store` 和迁移尚不存在，编译失败。

- [ ] **步骤 3：实施数据库结构和内嵌的单调迁移执行器**

初始 SQL 必须包含以下约束，时间戳以 RFC 3339 文本存储，UUID 以小写文本存储：

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

`Store::open` 使用 `SqliteConnectOptions`，启用缺失时创建、WAL、外键以及五秒忙等待超时。`migrate` 运行 `BEGIN IMMEDIATE`，若 `schema_migrations` 中不存在相应记录，则应用以 `(1, include_str!("../migrations/0001_initial.sql"))` 开头的有序表，插入版本号并提交。绝不自动删除或重建故障数据库。

- [ ] **步骤 4：实施仓库身份更新或插入（upsert）**

`register_repository` 启动立即事务并查找身份对；对于已有行，更新 `selected_path` 和 `last_opened_at`，否则插入一个 UUID v4 行。在 Windows 上，规范路径经过唯一的 `windows_identity_key` 函数，该函数统一分隔符并使用 Unicode 小写形式；测试必须覆盖大小写变体输入。Unix 身份键保留大小写。

- [ ] **步骤 5：运行聚焦测试和受影响的测试**

运行：`cargo test -p coding-agent-store --test migrations --test repositories`

运行：`cargo test -p coding-agent-domain -p coding-agent-store`

预期：所有测试通过。第二次迁移调用只留下一个版本行，重复注册只留下一个仓库行。

- [ ] **步骤 6：提交仓库持久化切片**

```bash
git add crates/coding-agent-store Cargo.lock
git commit -m "feat: persist registered repositories"
```

### 任务 3：添加原子任务/事件事务和投影

**文件：**
- 创建：`crates/coding-agent-store/src/tasks.rs`
- 创建：`crates/coding-agent-store/src/projection.rs`
- 创建：`crates/coding-agent-store/tests/tasks.rs`
- 创建：`crates/coding-agent-store/tests/projection.rs`
- 修改：`crates/coding-agent-store/tests/support/mod.rs`
- 修改：`crates/coding-agent-store/src/lib.rs`

**接口：**
- 产出：`TaskTransition`、`CreateTaskOutcome`、`RetryTaskOutcome`、`TransitionOutcome`、`AppendEventOutcome`、`RecoveryOutcome`、`BootstrapSnapshot`、`TaskDetail`、`EventPage`，以及存储层方法 `create_task`、`retry_task`、`transition_with_event`、`append_running_event`、`recover_incomplete`、`bootstrap_snapshot`、`task_detail`、`events_after`、`task_events_after` 和 `latest_event_id`。
- 不变量：每个改变可见任务状态的方法，都必须在同一事务中插入对应事件并更新 `last_event_id`。

- [ ] **步骤 1：编写失败的事务、重试、恢复和投影测试**

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

添加测试以验证：初次创建设置 `attempt = 1` 和 `retry_of = None`；不匹配的重复 `client_request_id` 返回 `IDEMPOTENCY_CONFLICT`；非法转换既不改变任务也不改变事件数量；启动恢复以原子方式中断所有 `Queued`/`Running` 任务；`TaskDetail` 在一个 SQLite 读事务中读取事件及全局水位线。

添加并发重试测试：针对一个终态来源同时释放至少八个调用，并只观察到一个直接子任务 ID/事件。对于每个生命周期事件，断言 `payload.task.last_event_id == event.id`。分别在任务状态更新、占位事件插入、末尾事件更新和最终载荷更新之后注入失败；每个故障都必须回滚整个事务，不得留下可发布事件或占位载荷。

- [ ] **步骤 2：运行聚焦测试并验证失败**

运行：`cargo test -p coding-agent-store --test tasks --test projection`

预期：因缺少任务方法和投影类型而编译失败。

- [ ] **步骤 3：实施任务创建、转换、事件追加和重试**

每次变更都使用 `BEGIN IMMEDIATE`。生命周期载荷内嵌最终 `Task`，因此所有创建/重试/转换/恢复路径都使用一个外部不可见的事务内序列：写入原始任务行/状态（创建时可以临时存储整数零而不构造领域 `Task`），插入带有有效 JSON 内部占位符 `{}` 的生命周期事件行，获取其 AUTOINCREMENT `EventId`，更新 `tasks.last_event_id`，重新加载并验证最终领域 `Task`，将该事件的占位符替换为包含重新加载 `Task` 的类型化载荷，验证恰好有一个载荷行发生变化，然后提交。任何公开/已提交 `Task` 的 ID 都不能为零，任何已提交事件都不能含占位符，并且每个生命周期载荷中的 `Task` 都必须回指自身事件 ID。

重复的创建请求 ID 要比较仓库和去除空白后的提示词。`transition_with_event(task_id, expected, transition)` 验证状态表，并在共享序列之前同时按 ID 和预期状态进行条件更新。非生命周期的运行中事件可立即插入其最终类型化载荷，然后在提交前更新 `Task.last_event_id`。调用者绝不提供生命周期载荷或过期的 `Task` 快照。

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

该封闭枚举使无效的失败组合无法表示。`Running` 设置 `started_at = now`，清除结束时间/失败信息，且只允许从 `Queued` 转入。每个终态转换都设置 `finished_at = now` 并保留现有 `started_at`；`Failed` 和 `Interrupted` 存储其结构化失败信息，而 `Completed`/`Cancelled` 将其清除。执行 SQL 之前，将状态表中的非法边拒绝为 `StoreError::IllegalTransition { from, to }`；CAS 未命中返回 `Conflict { current }`；ID 缺失返回 `StoreError::TaskNotFound`。创建不匹配返回 `StoreError::IdempotencyConflict`；重试非终态任务返回 `StoreError::TaskNotRetryable`。

`append_running_event` 只接受 `PlanUpdated`、`ActivityAppended`、`DiffUpdated` 或 `TestUpdated` 载荷；对于生命周期变体返回 `StoreError::InvalidRunningEvent`；除非任务仍为 `Running`，否则返回 `NotRunning`。初次创建始终设置 `attempt = 1` 和 `retry_of = None`。`retry_task` 只接受终态任务，插入前先返回已有的直接子任务，并且只创建一个新的 `Queued` 子任务及事件。子任务复制来源的 `repository_id` 和提示词，设置 `attempt = source.attempt + 1` 与 `retry_of = Some(source.id)`，并获得服务器新生成的 `ClientRequestId`；绝不复用来源请求 ID。创建/重试两个结果枚举均实施 `task(&self) -> &Task`。

`recover_incomplete(now, failure)` 在单一事务中更新所有 `Queued`/`Running` 任务，并按确定性的 `(created_at,id)` 顺序为每个任务插入一个 `task.interrupted` 事件，随后即使数量为零也返回数据库高水位线。调用者使用失败代码 `APP_RESTARTED`、`STORE_DEGRADED_RECOVERY` 或 `APP_SHUTDOWN`；每种代码都配有稳定且对用户安全的消息，并设置 `retryable = true`。

- [ ] **步骤 4：实施一致的引导快照和 TaskDetail 重放**

`bootstrap_snapshot` 在一个读事务中读取按 `(last_opened_at DESC,id)` 排序的所有仓库、按 `(created_at DESC,id)` 排序的所有任务摘要以及 `MAX(task_events.id)`；Project 1 不对任一列表分页或裁剪。`task_detail` 启动一个读事务，按 ID 顺序加载所有任务事件，投影面板状态，将同一快照的全局最大值读入 `event_cursor`，然后提交。

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

投影规则替换计划/差异/测试快照，按稳定条目 ID 追加活动，并且只从任务生命周期变体派生时间线。

- [ ] **步骤 5：运行存储层测试套件并验证原子行为**

运行：`cargo test -p coding-agent-store`

预期：所有迁移、仓库、任务、恢复和投影测试全部通过，零失败。

- [ ] **步骤 6：提交权威任务/事件存储层**

```bash
git add crates/coding-agent-store
git commit -m "feat: add atomic task event store"
```

### 任务 4：定义 API 契约和确定性 OpenAPI 导出

**文件：**
- 修改：`crates/coding-agent-api/Cargo.toml`
- 修改：`crates/coding-agent-api/src/lib.rs`
- 创建：`crates/coding-agent-api/src/contract.rs`
- 创建：`crates/coding-agent-api/src/backend.rs`
- 创建：`crates/coding-agent-api/src/error.rs`
- 创建：`crates/coding-agent-api/src/bin/export_openapi.rs`
- 创建：`crates/coding-agent-api/tests/openapi.rs`

**接口：**
- 使用：仅使用领域层模型；存储层投影/错误映射属于应用软件包，且本软件包不依赖 `coding-agent-store` 或 `coding-agent-app`。
- 产出：`UtcTimestampDto`、`CanonicalPathDto`、所有 REST DTO、基于判别器的 `TaskEventDto`、`StreamResetControl`、`ServiceStateControl`、`SseMessage`、`ApiError`、`ApiErrorResponse`、`CreateResult`、`CancelResult`、`QuitAcceptance`、`ApiBackend`、`SseBackend`、`RequestSecurity` 和 `ApiDoc`。
- 不变量：`Task` 事件载荷使用类型化的 OpenAPI `oneOf`；只有明确保持开放的 API 错误 `details` 映射可以使用 `serde_json::Value`。

API 清单增量必须严格如下：

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

- [ ] **步骤 1：编写失败的 OpenAPI 契约测试**

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

添加模式断言：`TaskDto.last_event_id` 必填且非空；`TaskDetail` 包含可为空的计划/差异/测试以及活动/时间线数组；`StreamResetControl` 严格由模式版本/种类/最新 ID 构成；`ServiceStateControl` 严格由模式版本/种类/状态/世代号构成。添加导出器集成测试：预先以哨兵字节创建输出，在同一路径上调用两次 `export_openapi`，并验证每次成功替换后，内容都是带规范字节的完整有效 JSON。端点路径/响应断言从任务 12 开始；届时真实的 `utoipa-axum` 路由器是唯一路径来源。

- [ ] **步骤 2：运行契约测试并验证红灯结果**

运行：`cargo test -p coding-agent-api --test openapi`

预期：由于 `ApiDoc` 和契约 DTO 尚不存在，编译失败。

- [ ] **步骤 3：实施精确的传输 DTO 和端口特征**

定义字段私有的 `UtcTimestampDto(String)` 和 `CanonicalPathDto(String)` 传输标量。它们唯一的构造函数使用领域层的 `UtcTimestamp`/`CanonicalPath`；时间戳序列化为 UTC RFC 3339，其 OpenAPI 模式为字符串/日期时间，而路径是平台字符串。DTO 映射绝不接受这些字段未经验证的任意字符串。

将 `TaskEventDto` 定义为覆盖十个具体事件封装结构体的 `#[serde(untagged)]` 枚举。每个事件封装都包含顶层 `id`、`schema_version`、`task_id`、单值 `kind` 枚举、类型化 `payload` 和 `created_at`；这会保留已批准的扁平传输帧，而不是将封装字段嵌套进载荷。将其 Utoipa 模式实施为带 `Discriminator::new("kind")` 的十个事件封装的 `oneOf`，并同时测试 JSON 结构和模式。将 `SseMessage` 定义为 `TaskEvent | StreamReset | ServiceState`，控制事件不携带持久化 ID。`BootstrapResponse` 包含 `csrf_token`、`repositories`、`tasks`、`latest_event_id`、`server_started_at`、`service_state`、`service_state_generation` 和 `max_concurrent_tasks`。

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

控制消息构造函数将模式版本固定为 `1`；其 `kind` 字段是单值枚举，`ServiceStateDto` 序列化为 `snake_case`。它们都没有 `id` 字段。

使用以下与传输无关的结果类型，使处理器无需检查领域层内部结构即可选择 `200` 或 `201`：

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

`QuitAcceptance::take_trigger(&mut self) -> Option<Box<dyn FnOnce() + Send + 'static>>` 只将回调移出一次。

后端端口必须公开以下严格一致的操作：

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

将重放/实时接缝保留在 API 软件包中，且不依赖应用执行体：

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

`events_between` 仅返回 `(after, through]` 范围内的持久化 ID，并按升序排列。`LiveEventItem::Lagged` 是从 SQLite 补充数据的信号，绝不序列化到浏览器。

请求安全端口必须严格一致且感知 HTTP，以便在处理器提取前拒绝重复的原始请求头：

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

分别定义内部错误和传输格式错误：

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

只有任务 12 的路由器注入请求 ID 并序列化 `ApiErrorResponse`。`CreateResult` 和 `CancelResult` 是内部控制结果，绝不是包装 JSON：`Created`/`Existing` 以 `201`/`200` 返回内部 DTO；`Finished` 以 `200` 返回 `Task` DTO；`Accepted` 以 `202` 返回 `{task,cancellation_requested:true}`。存储层/领域层失败由应用层 `ApplicationBackend` 映射到 `ApiError`，不得暴露秘密。

- [ ] **步骤 4：实施确定性 OpenAPI 导出**

`export_openapi` 只接受一个输出路径参数，将 `ApiDoc::openapi()` 序列化为格式化 JSON 并附加一个末尾换行，创建父目录，然后通过同目录下唯一命名的临时文件写入。发布前对临时文件执行刷新和 `sync_all`；绝不先删除目标文件。共享的 `atomic_replace` 在 Unix 上使用覆盖现有文件的 `rename`，在 Windows 上通过受目标平台条件限制的 `windows-sys` 使用 `MoveFileExW(MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)`，检查操作系统结果，并在平台支持时尽力同步父目录。每次发布失败时都清理临时文件。任务 12 在路径存在后将导出器切换到由真实路由器生成的 OpenAPI。

- [ ] **步骤 5：运行契约和工作区检查**

运行：`cargo test -p coding-agent-api --test openapi`

运行：`cargo run -p coding-agent-api --bin export_openapi -- web/openapi.json`

运行：`cargo run -p coding-agent-api --bin export_openapi -- target/openapi-check.json`

运行：`git diff --no-index --exit-code -- web/openapi.json target/openapi-check.json`

运行：`cargo test -p coding-agent-domain -p coding-agent-store -p coding-agent-api`

预期：OpenAPI 测试通过；`web/openapi.json` 存在，并且独立的第二次导出在字节层面完全一致。

- [ ] **步骤 6：提交 API 契约**

```bash
git add crates/coding-agent-api web/openapi.json Cargo.lock
git commit -m "feat: define local web api contract"
```

### 任务 5：串行化写入并管理服务状态

**文件：**
- 修改：`crates/coding-agent-app/Cargo.toml`
- 修改：`crates/coding-agent-app/src/lib.rs`
- 创建：`crates/coding-agent-app/src/service_state.rs`
- 创建：`crates/coding-agent-app/src/store_writer.rs`
- 创建：`crates/coding-agent-app/tests/store_writer.rs`
- 创建：`crates/coding-agent-app/tests/support/mod.rs`

**接口：**
- 使用：任务 2–3 中的所有存储层变更方法。
- 产出：`EventWake`、`StoreWriterHandle`、`StoreWriterError`、`WriteReceipt<T>`、`ServiceState::{Ready,StoreDegraded,Quiescing}`、`ServiceStateSnapshot { state, generation }` 和 `ServiceStateController`。
- 不变量：`store_writer.rs` 之外的应用程序代码只能获得只读 `Store`；每次变更都必须经过 `StoreWriterHandle`。

本任务的应用清单增量必须严格如下：

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

- [ ] **步骤 1：编写失败的 FIFO、瞬态重试和世代号测试**

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

每个应用集成测试都以 `mod support;` 开头；`tests/support/mod.rs` 负责假时钟、执行体测试夹具、受故障控制的 `Store` 适配器，以及供后续任务共享的构建器。添加故障注入测试：两次 `SQLITE_BUSY` 尝试之后成功；另添加一个测试，验证命令截止时间在事务尝试前到期时任务仍未提交。

使用可计数及会触发 panic 的假 `EventWake` 实现，断言每次已提交的任务/事件变更只通知一次，仅仓库写入或已回滚写入不通知，并且唤醒过程中的 panic 不能把持久提交变成 API 失败。

- [ ] **步骤 2：运行聚焦测试并确认红灯**

运行：`cargo test -p coding-agent-app --test store_writer`

预期：由于执行体和服务状态类型尚不存在，编译失败。

- [ ] **步骤 3：实施单一写入执行体**

`StoreWriterHandle` 通过有界 Tokio mpsc 通道发送封闭的 `WriteCommand` 枚举，并等待一次性通道。命令包括仓库注册、任务创建/重试、转换、运行中事件追加和未完成任务恢复。该执行体完成一个命令后才接收下一个。其构造函数通过本任务负责的以下端口接收 `std::sync::Arc<dyn EventWake>`：

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

仅仓库写入返回 `event_id = None`；单个任务/事件变更返回其已提交 `EventId`，批量恢复返回 `value.last_event_id`。写入器原样传递完整的存储层 `RecoveryOutcome`，使启动、降级恢复、关闭和 `EventDispatcher` 使用同一个高水位线定义。

只重试 `SQLITE_BUSY` 和 `SQLITE_LOCKED`，延迟依次为 25、50、100、200 和 400 毫秒。每个前台命令都带有截止时间。在首次事务之前及每次重试之前检查该时间；若它在一次失败/回滚的尝试后到期，则返回 `Busy`，且明确知道尚未提交。一次尝试开始后，绝不通过外部超时放弃其一次性通道：返回成功结果或已回滚失败；若 HTTP 客户端断开，则依靠原始请求 ID/CAS。命令在每次重试时使用同一请求 ID 或 CAS 条件。`Store` 返回已提交的任务/事件结果后，调用不含内容的 `EventWake`；生产者绝不发送 `TaskEvent` 对象。捕获并记录唤醒实现中的 panic，但仍返回持久回执，因为唤醒仅用于加速；任务 6 的定期数据库轮询是唤醒丢失后的恢复路径。

- [ ] **步骤 4：实施单一服务状态发布器**

`ServiceStateController` 负责一个 Tokio 监视发送端，以及一个受互斥锁保护的当前快照。`set(&self, next) -> Result<ServiceStateSnapshot, InvalidServiceTransition>` 仅在状态变化时递增世代号。合法边为 `Ready` ↔ `StoreDegraded`，以及两者之一 → `Quiescing`；设置为相同状态时返回未变的快照。`Quiescing` 是终态，因此尝试离开该状态会返回 `InvalidServiceTransition`。

- [ ] **步骤 5：运行聚焦测试和受影响的测试套件**

运行：`cargo test -p coding-agent-app --test store_writer`

运行：`cargo test -p coding-agent-domain -p coding-agent-store -p coding-agent-app`

预期：FIFO、重试、无歧义提交和世代号单调性测试通过。

- [ ] **步骤 6：提交写入和服务状态执行体**

```bash
git add crates/coding-agent-app Cargo.lock
git commit -m "feat: serialize application state writes"
```

### 任务 6：按数据库顺序发布持久化事件

**文件：**
- 创建：`crates/coding-agent-app/src/event_dispatcher.rs`
- 创建：`crates/coding-agent-app/tests/event_dispatcher.rs`
- 修改：`crates/coding-agent-app/tests/support/mod.rs`
- 修改：`crates/coding-agent-app/src/lib.rs`

**接口：**
- 使用：`Store::events_after`、`Store::latest_event_id` 以及任务 5 的 `EventWake` 端口。
- 产出：`EventDispatcherHandle::subscribe() -> broadcast::Receiver<TaskEvent>`、`EventDispatcherHandle::wake()`、`EventDispatcherHandle::flush_to(EventCursor)` 和 `impl EventWake for EventDispatcherHandle`。
- 不变量：只有此执行体能向实时广播通道发送持久化的 `TaskEvent` 值。

- [ ] **步骤 1：编写失败的排序和唤醒丢失测试**

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

- [ ] **步骤 2：运行聚焦测试并确认红灯**

运行：`cargo test -p coding-agent-app --test event_dispatcher`

预期：在 `EventDispatcherHandle` 处编译失败。

- [ ] **步骤 3：实施由游标管理的数据库轮询**

以启动恢复高水位线初始化游标。收到唤醒或每隔一秒时，反复查询 `events_after(cursor, 256)`，防御性地按 ID 排序，跳过不大于游标的 ID，广播每个事件，并且只在发送后更新游标。即使接收端数量为零，发送仍会推进游标，因为 SQLite 始终是重放来源。`flush_to(target)` 只在游标到达目标后才确认，否则返回存储层错误。

- [ ] **步骤 4：运行事件和存储层测试套件**

运行：`cargo test -p coding-agent-app --test event_dispatcher`

运行：`cargo test -p coding-agent-store -p coding-agent-app`

预期：排序、重复唤醒、唤醒丢失和刷新测试全部通过。

- [ ] **步骤 5：提交以数据库为后端的分发器**

```bash
git add crates/coding-agent-app
git commit -m "feat: publish durable events in order"
```

### 任务 7：实施 TaskManager 的领取、取消和静默化管理

**文件：**
- 创建：`crates/coding-agent-app/src/task_manager.rs`
- 创建：`crates/coding-agent-app/tests/task_manager.rs`
- 修改：`crates/coding-agent-app/tests/support/mod.rs`
- 修改：`crates/coding-agent-app/src/lib.rs`

**接口：**
- 使用：`StoreWriterHandle`、只读 `Store`、`ServiceStateController` 以及注入的 `Arc<dyn TaskRunner>`。
- 产出：`TaskManagerHandle::{notify_queued,cancel,quiesce_and_interrupt}`、`CancelOutcome`、`TaskRunner`、`RunContext`、`RunnerEvent`、`RunnerEventSink`、`RunnerEventError`、`RunnerOutcome`、`RunnerShutdownHandle` 和 `QuiesceResult`。
- 不变量：领取、取消、运行器事件/结果、状态协调和关闭屏障都是由同一执行体处理的消息；创建/重试继续通过 `StoreWriter` 串行化，且只在提交后通知此执行体。

在本任务中将 `async-trait.workspace = true` 添加到应用依赖。每个测试文件都通过 `mod support;` 导入共享测试夹具。

- [ ] **步骤 1：编写失败的许可和领取/取消竞态测试**

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

添加显式取消矩阵，分别暂停在：(a) 获取许可后但注册临时句柄前；(b) 注册句柄后但提交 `Running` 前；(c) 提交 `Running` 后但生成运行器前。任何一行都不得暴露没有令牌的 `Running`：若排队取消先提交，则运行器不会启动；若领取先提交，则触发已注册令牌，运行器通过正常取消结果退出。在领取 CAS 中注入 `BUSY` 和终态 `StoreWriter` 失败，并断言运行器未生成、临时句柄被移除、许可被释放、`Task` 保持 `Queued`，且状态协调随后恰好领取它一次。还要测试排队取消在领取前获胜、完成与取消之间首次提交获胜、队列通知丢失后的状态协调、FIFO `(created_at,id)`、拒绝迟到事件，以及运行器 panic 转换为 `RUNNER_PANICKED` 且不影响另一任务。

- [ ] **步骤 2：运行聚焦测试并验证红灯**

运行：`cargo test -p coding-agent-app --test task_manager`

预期：由于 TaskManager 和 TaskRunner 尚不存在，编译失败。

- [ ] **步骤 3：定义运行器端口和有界事件接收器**

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

`RunnerEventSink` 实现 `pub async fn append(&self, event: RunnerEvent) -> Result<EventId, RunnerEventError>`：它只通过有界消息将四种非生命周期变体发回执行体，并等待一次性通道的持久化结果，且不阻塞运行时线程。执行体仅通过 `append_running_event` 将其持久化，并返回已提交的事件 ID；终态任务返回 `TaskNotRunning`，降级模式返回 `StoreDegraded`，已关闭邮箱返回 `ManagerClosed`。通过此接收器无法表示运行器生命周期/终态事件。

- [ ] **步骤 4：实施执行体排序**

按 `(created_at,id)` 顺序扫描排队任务。使用 `Semaphore::try_acquire_owned`；若无可用许可，则让任务保持 `Queued` 并回到邮箱处理循环。获取后：注册临时令牌/许可，通过 `StoreWriter` 执行从 `Queued` 到 `Running` 的 CAS，仅在提交后生成运行器，并在 CAS 失败时清理。

取消消息由同一执行体决定。`Running` 取消会触发已注册令牌，重新读取最新 `Task`，并返回 `Accepted`；`Queued` 取消会提交并返回 `Cancelled`；已经是 `Cancelled` 时返回同一 `Cancelled`；`Completed`/`Failed`/`Interrupted` 返回 `TaskManagerError::TaskNotCancellable`。通过 `JoinError` 捕获已生成运行器的 panic，以 `status = Running` CAS 持久化一个终态结果，然后移除句柄并释放许可。

`quiesce_and_interrupt(deadline)` 停止扫描/领取，处理更早的邮箱消息，并通过 `StoreWriter` 执行一次从未完成到已中断的批量写入。它总是冻结执行体：提交时返回 `Durable`，携带 `Store` 恢复高水位线和活动句柄；发生已回滚/截止时间写入错误时返回 `Frozen`，携带相同活动句柄供降级关闭使用。每个运行器包装器都会在成功、失败、取消或 panic 时完成其 `done` 接收端。

- [ ] **步骤 5：运行任务管理器和持久化回归测试套件**

运行：`cargo test -p coding-agent-app --test task_manager`

运行：`cargo test -p coding-agent-store -p coding-agent-app`

预期：所有竞态测试均重复通过；使用 `cargo test -p coding-agent-app --test task_manager running_is_never_visible_without_an_active_handle -- --test-threads=1` 运行聚焦竞态测试 25 次，观察到零失败。

- [ ] **步骤 6：提交任务控制执行体**

```bash
git add crates/coding-agent-app Cargo.lock
git commit -m "feat: coordinate task lifecycle actor"
```

### 任务 8：添加确定性 FakeTaskRunner

**文件：**
- 创建：`crates/coding-agent-app/src/fake_runner.rs`
- 修改：`crates/coding-agent-app/src/lib.rs`
- 修改：`crates/coding-agent-app/tests/task_manager.rs`
- 修改：`crates/coding-agent-app/tests/support/mod.rs`

**接口：**
- 使用：任务 7 的 `TaskRunner`、`RunContext` 和 `RunnerEventSink`。
- 产出：`FakeTaskRunner`、`FakeRunnerConfig`，以及受功能特性限制、带有 `FakeScenario::{Success,Blocking,IgnoresCancellation,Failure,Panic}` 的 `ScriptedFakeRunner`。
- 不变量：生产环境行为是确定性的，绝不读取仓库内容或访问网络。

现在添加应用的 Cargo 功能特性，且必须早于任何测试引用脚本化行为：

```toml
[features]
default = []
test-support = []
```

- [ ] **步骤 1：使用暂停时间编写失败的确定性序列测试**

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

- [ ] **步骤 2：运行聚焦测试并验证红灯**

运行：`cargo test -p coding-agent-app --features test-support fake_runner_emits_the_approved_panel_sequence`

预期：由于 `FakeTaskRunner` 尚不存在，编译失败。

- [ ] **步骤 3：实施成功、取消、失败和 panic 场景脚本**

成功运行器依次发出一个完整的三项计划、三个稳定的活动条目、一个完整的合成差异快照、测试 `Running`，然后是测试 `Passed`；每次发出之间的间隔可配置，默认为 200 毫秒。在每个间隔之前和之后检查取消令牌。生产构造函数始终选择 `Success`。

在 Cargo 功能特性 `test-support` 下，`ScriptedFakeRunner` 按任务创建顺序使用进程加载的显式场景队列。它不检查提示词文本，也不公开 HTTP 控制路由。`Blocking` 等待取消或测试释放通道；`IgnoresCancellation` 只等待其测试释放通道，以便证明关闭预算；`Failure` 返回固定的 `FAKE_RUNNER_FAILURE`；`Panic` 为隔离测试而故意触发 panic。

- [ ] **步骤 4：运行假运行器和任务管理器测试**

运行：`cargo test -p coding-agent-app --features test-support fake_runner`

运行：`cargo test -p coding-agent-app --features test-support --test task_manager`

预期：确定性序列、取消、失败和 panic 隔离测试均通过，且无需按真实时钟休眠。

- [ ] **步骤 5：提交假执行切片**

```bash
git add crates/coding-agent-app
git commit -m "feat: add deterministic fake task runner"
```

### 任务 9：协调 StoreDegraded 恢复

**文件：**
- 创建：`crates/coding-agent-app/src/shutdown.rs`
- 创建：`crates/coding-agent-app/tests/degraded_recovery.rs`
- 修改：`crates/coding-agent-app/src/store_writer.rs`
- 修改：`crates/coding-agent-app/src/task_manager.rs`
- 修改：`crates/coding-agent-app/src/lib.rs`
- 修改：`crates/coding-agent-app/tests/support/mod.rs`

**接口：**
- 使用：存储层的 `RecoveryOutcome` 及其持久 `high_watermark`。
- 产出：`DegradedCoordinator::run`、`PendingDurableResult` 和应用层 `DegradedRecoveryResult`。
- 不变量：后台写入耗尽重试次数后，在所有状态不明确的 `Queued`/`Running` 任务被持久转换为 `Interrupted` 且服务状态回到 `Ready` 之前，不得启动新运行器。

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

待处理值是诊断/所有权标记，不是第二个持久队列。一旦批量恢复为每个状态不明确的任务提交 `Interrupted`，且分发器刷新到 `recovery.high_watermark`，协调器就会丢弃它们，并返回设置 `Ready` 时产生的世代号。

- [ ] **步骤 1：编写失败的后台终态写入测试**

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

- [ ] **步骤 2：运行聚焦测试并验证红灯**

运行：`cargo test -p coding-agent-app --test degraded_recovery`

预期：由于后台 StoreWriter 错误未得到协调，测试失败。

- [ ] **步骤 3：实施降级进入和恢复顺序**

当运行器事件或终态持久化耗尽有界重试次数时，在内存中保留待处理结果，设置 `StoreDegraded`，停止状态协调/领取，并取消所有活动令牌。每秒通过 `StoreWriter` 重试一次批量 `recover_incomplete` 事务。只有在事务提交且 `EventDispatcher` 刷新到其高水位线后，协调器才能清除待处理结果、重启状态协调，并以更大的世代号设置 `Ready`。

当前台变更超时且已知命令未提交时，返回 `503 STORE_BUSY`，不进入此协调器。非瞬态损坏保持 `StoreDegraded`，绝不删除/重建数据库。

- [ ] **步骤 4：运行降级、管理器、写入器和分发器测试套件**

运行：`cargo test -p coding-agent-app --test degraded_recovery --test store_writer --test event_dispatcher --test task_manager`

预期：恢复顺序为提交 `Interrupted` 事件 → 分发器完成刷新 → 发布 `Ready` 世代号；处于降级状态时不启动任何 `Queued` 任务。

- [ ] **步骤 5：提交降级模式协调器**

```bash
git add crates/coding-agent-app
git commit -m "feat: recover from task store outages"
```

### 任务 10：添加跨平台路径、仓库发现和原生适配器

**文件：**
- 修改：`crates/coding-agent-app/Cargo.toml`
- 修改：`crates/coding-agent-app/src/lib.rs`
- 创建：`crates/coding-agent-app/src/platform.rs`
- 创建：`crates/coding-agent-app/src/repository_service.rs`
- 创建：`crates/coding-agent-app/src/native_dialog.rs`
- 创建：`crates/coding-agent-app/tests/platform.rs`
- 创建：`crates/coding-agent-app/tests/repository_service.rs`
- 修改：`crates/coding-agent-app/tests/support/mod.rs`

**接口：**
- 产出：`PlatformPaths`、`PrivateFile`、`BrowserLauncher`、`CommandRunner`、`RepositoryDiscovery`、`DiscoveredRepository`、`NativeDialogService` 和 `PickerError`。
- 不变量：发现功能只能运行 `git rev-parse` 和 `cargo locate-project`；绝不读取源代码内容、解析依赖、构建代码或更改仓库。

添加以下应用清单增量：

```toml
[dependencies]
directories.workspace = true
rfd.workspace = true
webbrowser.workspace = true

[target.'cfg(windows)'.dependencies]
windows-sys.workspace = true
```

- [ ] **步骤 1：编写失败的路径、权限、发现和选择器测试**

创建临时仓库，覆盖以下情形：嵌套的选定目录、位于选定目录与 Git 根目录之间的清单、Git 根目录之外的 Cargo 工作区、缺失清单、`Cargo.lock` 缺失/过期/脏状态、仓库 `rust-toolchain.toml` 指定一个故意不可用的通道、通过符号链接选择、不存在的选定路径，以及将普通文件当作目录选择。断言最后两种情形在调用任一命令前就失败，分别返回 `REPOSITORY_PATH_NOT_FOUND` 和 `REPOSITORY_PATH_NOT_DIRECTORY`。发现前记录递归相对文件列表、每个现有锁文件的字节序列以及 `git status --porcelain=v1`，并断言发现后在字节层面完全一致。不可用工具链测试夹具仍必须成功定位，以证明 Cargo 从中立运行时当前工作目录执行，而非激活仓库覆盖配置。

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

`platform.rs` 测试必须断言数据/运行时目录和敏感文件均为私有：Unix 模式为 `0700`/`0600`；Windows 所有者 DACL 授权当前用户并拒绝继承的宽泛访问权限。浏览器启动失败必须可观察，但不得成为进程致命错误。

- [ ] **步骤 2：运行聚焦测试并验证红灯**

运行：`cargo test -p coding-agent-app --test platform --test repository_service`

预期：由于平台和发现端口尚不存在，编译失败。

- [ ] **步骤 3：实施应用程序路径和私有文件辅助函数**

`PlatformPaths::discover()` 使用 `directories::ProjectDirs::from("com", "ngy", "coding-agent")` 获取用户本地数据路径。操作系统运行时目录可用时优先使用，否则使用 `<data_local>/run`。它公开 `database_path`、永久的 `instance.lock`、可替换的 `instance.json` 和 `unclean-shutdown.json`。目录创建是幂等的。敏感文件使用 `create_new` 创建，在发布内容前应用仅所有者权限，并且绝不跟随最终路径处的符号链接。Windows 权限代码隔离在 `cfg(windows)` 后并使用 `windows-sys`；Unix 使用 `OpenOptionsExt` 和 `PermissionsExt`。

- [ ] **步骤 4：实施只读仓库发现**

启动任何子进程前，使用 `symlink_metadata`/`metadata` 检查选定路径：将路径不存在映射为 `REPOSITORY_PATH_NOT_FOUND`，将非目录拒绝为 `REPOSITORY_PATH_NOT_DIRECTORY`，随后规范化并标准化该目录。通过可注入的 `CommandRunner` 严格运行以下流程：

1. `git -C <selected> rev-parse --show-toplevel`。
2. 从选定目录沿祖先目录遍历至规范化 Git 根目录，并选择第一个存在的 `Cargo.toml`。
3. 将 `PlatformPaths::runtime_dir` 作为中立的子进程工作目录，运行 `cargo locate-project --workspace --manifest-path <manifest> --message-format plain`。
4. 规范化返回的清单父目录；除非路径组件包含关系证明它位于 Git 根目录内，否则拒绝。

不要使用字符串前缀判断包含关系。将命令生成失败、非零退出、无效 UTF-8、根目录缺失和根目录外工作区转换为稳定代码，不向 API 调用者返回原始标准错误输出。单元测试证明无效选定路径既不调用 Git 也不调用 Cargo。集成测试使用真实 Git/Cargo，并确认文件未发生变化。

- [ ] **步骤 5：实施浏览器和串行化的原生对话框适配器**

`BrowserLauncher::open` 只将完整的 `http://127.0.0.1:<port>/#token=<token>` URL 委托给强化安全的 `webbrowser`。失败时返回 URL 供调用者的原生错误对话框使用，且不停止服务器。`NativeDialogService` 负责一个原子操作/互斥锁关卡，并通过平台支持的异步适配器调用 `rfd`，包括 macOS 所需的主线程/事件循环交接。取消返回 `Ok(None)`，并发进入返回 `PickerError::AlreadyOpen`，处理器绝不直接调用 `rfd`。固定并测试稳定的发现/对话框代码 `REPOSITORY_PATH_NOT_FOUND`、`REPOSITORY_PATH_NOT_DIRECTORY`、`CARGO_WORKSPACE_NOT_FOUND`、`CARGO_WORKSPACE_OUTSIDE_GIT_ROOT`、`REPOSITORY_COMMAND_FAILED` 和 `PICKER_ALREADY_OPEN`。

- [ ] **步骤 6：运行平台回归测试并提交**

运行：`cargo test -p coding-agent-app --test platform --test repository_service`

运行：`cargo test -p coding-agent-store -p coding-agent-app`

预期：所有平台和发现测试通过，且真实测试夹具指纹保持不变。

```bash
git add crates/coding-agent-app Cargo.lock
git commit -m "feat: add local platform and repository discovery"
```

### 任务 11：实现进程范围的会话、Host、Origin 和 CSRF 安全机制

**文件：**
- 创建：`crates/coding-agent-app/src/security.rs`
- 创建：`crates/coding-agent-app/tests/security.rs`
- 修改：`crates/coding-agent-app/src/lib.rs`
- 修改：`crates/coding-agent-app/tests/support/mod.rs`

**接口：**
- 产出：`SecuritySeed`、`SecurityManager`、`LaunchToken`、`LauncherSecret`、`SessionRecord`，以及 API `RequestSecurity` 的应用层实现。
- 不变量：除仅所有者可读的运行时描述符中的启动器密钥外，所有密钥都只存在于内存中；任何密钥都不得进入 SQLite、请求目标或常规日志。

向应用依赖添加 `axum-extra.workspace = true`、`base64.workspace = true`、`getrandom.workspace = true`、`http.workspace = true` 和 `subtle.workspace = true`，并向应用开发依赖添加 `tracing-subscriber.workspace = true`，用于捕获日志并验证敏感信息已被脱敏。

- [ ] **步骤 1：编写失败的一次性令牌和请求边界测试**

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

添加表格驱动测试，覆盖精确 Host、Origin 缺失或来自外部、CSRF 缺失或错误、伪造或旧的 Cookie、通过伪时钟验证两分钟过期、进程重启后失效、公开读取授权、变更授权，以及启动器密钥的常量时间比较。捕获预先植入已知令牌、启动器密钥、Cookie 和 CSRF 值的跟踪输出，断言常规信息级日志不包含其中任何字节。断言响应不含 `Access-Control-Allow-Origin` 标头。

- [ ] **步骤 2：运行聚焦测试并验证红灯**

运行：`cargo test -p coding-agent-app --test security`

预期：由于 `SecurityManager` 和 `RequestSecurity` 实现尚不存在，编译失败。

- [ ] **步骤 3：实现密钥签发和原子交换**

每个启动令牌、启动器密钥、会话 ID 和 CSRF 令牌都由 `getrandom` 填充的 32 字节生成，再以 URL 安全且无填充的 base64 编码。`SecuritySeed::generate` 在绑定端口前创建进程密钥；确定环回端口后，`SecurityManager::from_seed(seed, public_origin, clock)` 只消费该种子一次。启动令牌存放在一个由互斥锁保护的映射中，并记录签发和过期时刻。交换流程验证精确 Host 和已配置的公共 Origin，在持有映射锁时移除有效令牌，然后创建独立会话。使用 `subtle::ConstantTimeEq` 比较提交的密钥和令牌。

返回仅限主机的 `coding_agent_session` cookie，并设置 `HttpOnly`、`SameSite=Strict` 和 `Path=/`；由于生产环境使用环回 HTTP，因此省略 `Domain`、`Expires` 和 `Secure`。JavaScript 只能从通过身份验证的引导响应中取得 CSRF 令牌。新建的 `SecurityManager` 不得知晓任何先前进程的令牌、cookie、CSRF 值或启动器密钥。

- [ ] **步骤 4：实现三种授权级别**

按以下规则实现 API `RequestSecurity`：

- 交换：精确 Host、精确公共 Origin 和一个有效启动令牌；
- 读取/SSE：精确 Host 和一个有效的会话 cookie；
- 变更：读取检查，再加上精确 Origin 和以常量时间匹配的 `X-CSRF-Token`。

内部端点 `/_local/ready` 和 `/_local/reopen` 使用精确 Host 加 `X-Launcher-Secret`，绝不使用浏览器 cookie。拒绝重复的安全标头、生产环境中配置的非环回 Origin，以及 `localhost` 等任何 Host 别名。请求诊断只能记录生成的请求 ID 和稳定错误代码。

开发模式只接受唯一且显式配置的 Vite 公共 Origin 和代理 Host。它执行与生产环境相同的会话、CSRF、启动器密钥和变更关卡检查；不存在调试身份验证绕过，也不存在通配的 `localhost` 规则。

- [ ] **步骤 5：运行安全和契约测试套件**

运行：`cargo test -p coding-agent-app --test security`

运行：`cargo test -p coding-agent-api -p coding-agent-app`

预期：并发交换测试恰有一次成功；负向矩阵中的每一行都以拒绝方式安全失败。

- [ ] **步骤 6：提交安全边界**

```bash
git add crates/coding-agent-app Cargo.lock
git commit -m "feat: protect local browser sessions"
```

### 任务 12：接入受保护的 REST API 和应用后端

**文件：**
- 创建：`crates/coding-agent-api/src/router.rs`
- 创建：`crates/coding-agent-api/tests/router.rs`
- 创建：`crates/coding-agent-api/tests/support/mod.rs`
- 创建：`crates/coding-agent-app/src/server.rs`
- 创建：`crates/coding-agent-app/tests/server.rs`
- 修改：`crates/coding-agent-api/src/lib.rs`
- 修改：`crates/coding-agent-app/src/lib.rs`
- 修改：`crates/coding-agent-app/tests/support/mod.rs`

**接口：**
- 产出：`build_api_router`、`ApplicationBackend`、`MutationGate`，以及从 REST 到领域层/存储层错误的精确映射。
- 不变量：路由处理器只包含传输层映射；变更操作必须经过 TaskManager/StoreWriter，仓库发现必须经过任务 10 的服务。

向 API 开发依赖添加 `http-body-util.workspace = true`、`tokio.workspace = true` 和 `tower.workspace = true`。向应用依赖添加 `axum.workspace = true`、`http-body-util.workspace = true`、`tower.workspace = true` 和 `tower-http.workspace = true`；任务 11 已将 `http` 加为应用的直接依赖。每个 API 集成测试都以 `mod support;` 开头；该模块负责伪后端、安全/SSE 端口和响应解码器。

- [ ] **步骤 1：使用伪端口编写失败的路由矩阵测试**

API 路由测试提供伪 `ApiBackend`、`RequestSecurity` 和 `SseBackend` 实现，并覆盖每条路由、每种方法、身份验证级别、内容类型、请求 ID 以及成功/错误状态。断言路由器生成的 OpenAPI 恰好包含已批准的 `/api/session/exchange`、引导、仓库、任务/详情/取消/重试/任务事件、全局事件和应用退出路径；取消接口应记录 `200 TaskDto` 和 `202 CancellationAcceptedResponse`。包括以下变更操作断言：

- 创建/选择仓库：新建返回 `201`，已存在返回 `200`，取消选择器返回 `204`，选择器繁忙返回 `409`；
- 创建任务：首次返回 `201`，等价的幂等重放返回 `200`，冲突重放返回 `409`，空白或超过 50,000 个 Unicode 标量值返回 `422`；
- 取消：Queued 返回 `200`，Running 返回 `202`，Cancelled 返回 `200`，其他终态返回 `409`；
- 重试：首个子任务返回 `201`，同一个直接子任务返回 `200`，非终态返回 `409`；
- 有界耗尽 SQLite BUSY/LOCKED 重试：返回 `503 {code:"STORE_BUSY",retryable:true}`，且不提交任何变更；
- 已接受退出：返回 `202 {"status":"shutting_down"}`；已关闭/降级的数据变更关卡：返回稳定的 `503`。

添加使用相同请求 ID 的并发测试，证明只创建一个任务/事件，且两个响应引用同一个 Task。

添加并发重试测试，证明针对同一个终态源任务的多个请求会返回同一个直接子任务，其中一个响应为 `201`，其余为 `200`。当服务状态为 StoreDegraded 时，验证仓库/任务变更返回 `503 STORE_DEGRADED`，但受保护的退出端点仍然可用，以便用户进入降级关闭流程；测试可以直接植入 Store 状态，但不得调用绕过检查的公开入队路径。

捕获包含已知提示词和规范路径的请求所产生的服务器信息级日志；断言日志只包含稳定的请求/仓库/任务 ID 和错误代码，不包含提示词或完整路径。显式启用的本地调试日志可以将路径视为用户数据，但仍绝不能输出会话密钥。

- [ ] **步骤 2：运行路由测试并验证红灯**

运行：`cargo test -p coding-agent-api --test router`

预期：由于路由器尚未定义，编译失败。

- [ ] **步骤 3：基于携带 OpenAPI 元数据的处理器实现 API 路由器**

使用 `utoipa-axum` 构建路由，使运行时处理器的路径/方法和导出的 OpenAPI 来自同一份注册信息。`api_openapi()` 构造同一个未绑定的 `OpenApiRouter<ApiState>` 并返回其文档；`build_api_router` 提供状态并对外服务；`export_openapi` 现在调用 `api_openapi()`，而非任务 4 中只有组件的文档。在整个路由器外层应用精确 Host 验证；逐端点应用读取或变更授权。`POST /api/session/exchange` 成功时返回 `204` 和 `Set-Cookie`。包括拒绝和 panic 在内的每个响应都包含 `X-Request-Id`；绝不回显格式错误的传入 ID。

将 `CreateResult` 映射为 `201`/`200`。将稳定的应用错误映射到已批准的 JSON 封装，并且绝不包含命令标准错误、密钥、提示词文本，也不暴露已获授权的仓库 DTO 之外的文件系统内部信息。不要添加 CORS 中间件。

- [ ] **步骤 4：实现 `ApplicationBackend` 和变更关卡入口**

`ApplicationBackend` 将引导/列表/详情/事件映射到一个只读 Store，并通过 SecurityManager 解析已认证会话的 CSRF。它通过 StoreWriter 发送创建/重试操作，通过 TaskManager 发送取消操作。入队前先去除提示词首尾空白并统计 Unicode 标量值。创建/重试成功后，只有提交完成才通知 TaskManager；协调流程允许通知丢失。仓库路径路由和选择器路由共用 RepositoryDiscovery 与 StoreWriter 注册逻辑。所有存储层/领域层错误都在此处转换为 API 自有错误，绝不放在 API 软件包中处理。

`MutationGate::enter_data_mutation()` 在 Ready 时返回 RAII 守卫，在降级时返回 STORE_DEGRADED，关闭后返回 APP_SHUTTING_DOWN。Ready 或 StoreDegraded 状态下都允许调用 `prepare_quit()`，仅在 Quiescing 开始后才拒绝。退出操作通过响应体包装器返回其 `202` 响应体，该包装器在流结束回调中发送关闭信号；集成测试必须先收到完整响应，监听器才能开始静默退出。

- [ ] **步骤 5：运行 API、服务器、管理器和存储测试套件**

运行：`cargo test -p coding-agent-api --test router`

运行：`cargo test -p coding-agent-app --test server --test task_manager --test store_writer`

运行：`cargo run -p coding-agent-api --bin export_openapi -- web/openapi.json`

运行：`cargo run -p coding-agent-api --bin export_openapi -- target/openapi-check.json`

运行：`git diff --no-index --exit-code -- web/openapi.json target/openapi-check.json`

预期：所有路由矩阵均通过；新增路由路径会按预期更新受版本控制的契约，且两次全新导出的文件在字节层面完全相同。

- [ ] **步骤 6：提交受保护的命令/查询层**

```bash
git add crates/coding-agent-api crates/coding-agent-app web/openapi.json Cargo.lock
git commit -m "feat: expose protected local rest api"
```

### 任务 13：实现无缺口的 SSE 重放和实时流

**文件：**
- 创建：`crates/coding-agent-api/src/sse.rs`
- 创建：`crates/coding-agent-api/tests/sse.rs`
- 修改：`crates/coding-agent-api/src/router.rs`
- 修改：`crates/coding-agent-api/src/lib.rs`
- 修改：`crates/coding-agent-app/src/server.rs`
- 修改：`crates/coding-agent-app/tests/server.rs`
- 修改：`crates/coding-agent-api/tests/support/mod.rs`
- 修改：`crates/coding-agent-app/tests/support/mod.rs`

**接口：**
- 消费：任务 4 的 `SseBackend`、任务 6 的 `EventDispatcherHandle`、只读 Store 重放，以及 `ServiceStateController` 监视通道。
- 产出：通过身份验证的 `GET /api/events?after=<id>`，其中包含持久化的任务帧和非持久化的控制帧。
- 不变量：最后发出的持久化 ID 严格递增；服务状态控制帧和心跳绝不改变该 ID。

向 API 依赖添加 `async-stream.workspace = true` 和 `tokio.workspace = true`。向应用依赖添加 `futures-util.workspace = true`、`tokio-stream.workspace = true` 和 `async-stream.workspace = true`，用于端口适配器。

- [ ] **步骤 1：编写失败的接合、重叠、重置、滞后和心跳测试**

使用确定性的伪 `SseBackend`，它能够在订阅、高水位读取、历史积压分页和实时队列排空之间暂停。覆盖以下情形：在各个暂停点提交事件、同一事件同时出现在历史积压与实时流中、实时缓冲区乱序、广播滞后、游标大于数据库最大值、服务代次在引导和 SSE 之间发生变化，以及在暂停时间下每 15 秒发送一次心跳。

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

- [ ] **步骤 2：运行 SSE 测试并验证红灯**

运行：`cargo test -p coding-agent-api --test sse`

预期：由于 SSE 接合流程尚未实现，编译失败。

- [ ] **步骤 3：实现初始服务控制和持久化流接合**

读取授权通过后，先订阅服务状态流和实时任务流。立即发出当前的 `ServiceStateControl`。读取当前最大 ID；如果 `after` 更大，则发出不含 `id` 字段的 `event: stream.reset` 并关闭。否则分页读取 `events_between(after, high, 256)`，按升序发出直至 `high` 的 ID，对缓冲的实时项目排序并去重，然后继续实时发送，同时跳过所有不大于最后已发送 ID 的项目。

传输格式使用持久化的 `id`、领域事件名称和单行 JSON 数据。`stream.reset` 和 `service.state` 不带持久化 ID。遇到未知内部错误时，先记录不序列化任何密钥的诊断日志，再终止数据流。

- [ ] **步骤 4：从广播滞后中恢复并合并服务状态**

收到 `LiveEventItem::Lagged` 时，查询新的最大值，从最后已发送的 ID 开始重新填充 SQLite 分页，然后再继续消费实时流。持续追赶直至同步；绝不合成任务事件。合并服务状态监视通道的变更，仅发送代次大于上一个服务代次的状态。每 15 秒交错发送一次 `: heartbeat` 注释，同时确保两个来源都不会得不到处理。

- [ ] **步骤 5：运行聚焦和进程级 SSE 回归测试**

运行：`cargo test -p coding-agent-api --test sse`

运行：`cargo test -p coding-agent-app --test event_dispatcher --test server`

预期：在历史积压/实时流重叠及广播滞后时，持久化输出仍严格递增且没有缺口；重置会关闭连接；心跳不携带 ID。

- [ ] **步骤 6：提交可重放的 SSE**

```bash
git add crates/coding-agent-api crates/coding-agent-app Cargo.lock
git commit -m "feat: stream replayable task events"
```

### 任务 14：组装主进程与次进程的单实例启动流程

**文件：**
- 创建：`crates/coding-agent-app/src/single_instance.rs`
- 创建：`crates/coding-agent-app/src/main.rs`
- 创建：`crates/coding-agent-app/tests/single_instance.rs`
- 修改：`crates/coding-agent-app/src/platform.rs`
- 修改：`crates/coding-agent-app/src/server.rs`
- 修改：`crates/coding-agent-app/src/lib.rs`
- 修改：`crates/coding-agent-app/tests/support/mod.rs`

**接口：**
- 产出：`InstanceLock`、`RuntimeDescriptor`、`PrimaryRuntime`、`SecondaryRuntime`、`StartupPhase`、`/_local/ready` 和 `/_local/reopen`。
- 不变量：打开 SQLite 前必须确定锁的所有权；次进程绝不构造 StoreWriter 或 TaskManager。

向应用依赖添加 `serde.workspace = true`、`serde_json.workspace = true`、`time.workspace = true` 和 `uuid.workspace = true`，并将现有的 `tracing-subscriber.workspace = true` 条目从应用开发依赖提升为正式依赖。

- [ ] **步骤 1：编写失败的锁、描述符和启动阶段测试**

测试以下阶段矩阵：应用数据目录创建/权限失败、发布描述符前已持有锁、就绪状态为 Starting 时发布描述符、Ready 状态下重新打开、描述符格式错误、启动器密钥错误、描述符指向已终止进程、浏览器打开失败，以及 10 秒超时。不可写路径测试注入平台文件系统错误，并断言只显示一条原生错误消息、进程非零退出，且不创建锁/数据库/监听器。注入一个一旦调用就 panic 的 Store 工厂，断言次进程不会打开数据库连接。

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

- [ ] **步骤 2：运行单实例测试并验证红灯**

运行：`cargo test -p coding-agent-app --test single_instance`

预期：由于锁和描述符类型尚不存在，编译失败。

- [ ] **步骤 3：实现永久锁文件所有权和描述符原子发布**

以读取/写入/创建模式打开永久的 `instance.lock`，并调用稳定版 `std::fs::File::try_lock`。在主进程的整个生命周期内保持该文件描述符有效；绝不删除或重命名锁文件。只有锁所有者才能移除过期描述符。

先将 `RuntimeDescriptor { instance_id, pid, port, started_at, launcher_secret }` 发布到同级私有临时文件，调用 `sync_all`，再原子重命名为 `instance.json`，并在平台支持时同步父目录。读取方每次重试后都重新打开文件，绝不读取临时路径。联系主进程前，验证字段边界、环回端口、UUID、PID 和仅所有者权限。

- [ ] **步骤 4：实现严格的主进程组装顺序**

主进程按以下顺序执行：路径 → 加锁 → 清理过期描述符 → 打开/迁移 Store → 原子恢复未完成任务 → 生成 SecuritySeed → 绑定 `127.0.0.1:0` → 使用精确的已绑定 Origin 构造 SecurityManager → 在恢复出的高水位初始化事件分发器 → 启动 StoreWriter/TaskManager → 以 Starting 模式提供服务 → 使用启动器密钥自探测 `/_local/ready` → 设置为 Ready → 发布描述符 → 打开带片段的 URL。

应用数据目录/运行时目录创建或权限失败时，显示原生错误，并在创建锁/数据库/监听器前退出。迁移/恢复失败时，显示原生错误，并在发布监听器前退出。绑定重试次数必须有限。浏览器打开失败时保持服务器运行，并通过原生消息适配器显示可完整复制的 URL。Starting 模式只公开受启动器密钥保护的就绪探测；公共 API 和静态资源请求返回 `503 APP_STARTING`。

- [ ] **步骤 5：实现不创建第二个写入器的次进程重新打开流程**

发生锁争用时，以有界指数延迟重试读取描述符以及受启动器密钥保护的就绪/重新打开请求，总时长最多 10 秒。`/_local/ready` 只返回实例 ID 和状态。`/_local/reopen` 在 Ready 前返回 `503`；进入 Ready 后签发一个新的、有效期两分钟的一次性浏览器令牌，并返回完整的带片段 URL。打开该 URL 并以零状态码退出。如果无法验证持锁的主进程，则显示明确错误，并保持锁/描述符不变。

- [ ] **步骤 6：运行启动和执行体回归测试套件**

运行：`cargo test -p coding-agent-app --test single_instance --test server --test task_manager --test event_dispatcher`

预期：所有阶段交错测试均通过；次进程测试证明不会构造 Store。

- [ ] **步骤 7：提交可执行程序的组装根节点**

```bash
git add crates/coding-agent-app Cargo.lock
git commit -m "feat: launch one protected local instance"
```

### 任务 15：实现优雅关闭和降级关闭

**文件：**
- 修改：`crates/coding-agent-app/src/shutdown.rs`
- 修改：`crates/coding-agent-app/src/server.rs`
- 修改：`crates/coding-agent-app/src/task_manager.rs`
- 修改：`crates/coding-agent-app/src/event_dispatcher.rs`
- 修改：`crates/coding-agent-app/src/single_instance.rs`
- 创建：`crates/coding-agent-app/tests/shutdown.rs`
- 修改：`crates/coding-agent-app/tests/support/mod.rs`

**接口：**
- 产出：`ShutdownCoordinator`、`ShutdownOutcome::{Clean,Degraded}`，以及信号/HTTP 关闭来源。
- 不变量：持久化关闭阶段最多占用 5 秒，整个关闭流程最多占用 10 秒；每条路径都必须尝试清理描述符和锁。

- [ ] **步骤 1：编写失败的静默化交错和时间预算测试**

在每个关卡/执行体边界暂停创建、重试、领取、运行器事件、运行器结果和退出操作。断言在关卡关闭前进入的操作，要么在 TaskManager 屏障前提交，要么以确定方式失败；越过屏障后，任何迟到的写入都不能留下 Queued/Running 状态。添加 Store 永久失败且标记文件创建也失败的测试，并断言在虚拟时间到达 10 秒前作出进程退出决定，同时已移除描述符并释放锁。使用 `FakeScenario::IgnoresCancellation` 运行 Store 正常路径：持久化屏障必须先持久化 Interrupted，等待 `done` 必须在总时间预算的剩余时间耗尽时停止，而且进程仍必须在 10 秒前选择退出。

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

- [ ] **步骤 2：运行聚焦测试并验证红灯**

运行：`cargo test -p coding-agent-app --features test-support --test shutdown`

预期：由于尚无协调器负责完整流程，关闭顺序/时间预算断言失败。

- [ ] **步骤 3：实现正常静默化顺序**

只接收一次 Ctrl-C、操作系统终止信号或延迟发送的 Web UI 退出信号。将服务状态设为 Quiescing，关闭 MutationGate，等待所有现有守卫退出，然后向 TaskManager 发送 `quiesce_and_interrupt(deadline)` 作为 FIFO 屏障。匹配 `QuiesceResult::Durable` 后，取消其中的活动令牌，只在剩余时间预算内等待其 `done` 接收端，拒绝迟到的事件/结果 CAS，将 EventDispatcher 刷新至 `recovery.high_watermark`，对 SQLite 句柄执行检查点并关闭，停止接受 HTTP，原子移除描述符，最后释放持锁文件句柄。若匹配到 Frozen 或该阶段达到 5 秒超时，则进入步骤 4 的回退流程。

保留在屏障前已提交的 Completed/Failed 状态。批量关闭事务中仍为 Queued/Running 的任务都变为 Interrupted，绝不变为 Cancelled。关闭或刷新浏览器不得产生关闭信号。

- [ ] **步骤 4：实现降级回退和诊断标记**

将持久化阶段限制在 5 秒以内。如果超时或 Store 永久损坏，则在内存中冻结 TaskManager，取消所有令牌，关闭事件接收器/监听器，并尽力写入一个只包含时间戳、实例 ID 和稳定错误代码的私有标记。无论标记创建成功还是失败，都要在 10 秒总截止时间前移除描述符、释放锁并选择非零退出码。

此后每次主进程启动，无论标记是否存在，都运行正常的未完成任务恢复。只有恢复提交后才移除标记。数据库打开/迁移失败时，保持数据库和标记不变并退出，不进入重启循环。降级退出前，发布/记录一条用户可见的稳定消息，说明部分任务终态无法持久化；绝不声称已干净关闭。

- [ ] **步骤 5：运行关闭和恢复回归测试**

运行：`cargo test -p coding-agent-app --features test-support --test shutdown --test degraded_recovery --test task_manager --test server --test single_instance`

预期：每种交错执行最终都保持一致；正常和降级路径均满足各自的虚拟时间预算。

- [ ] **步骤 6：提交生命周期关闭流程**

```bash
git add crates/coding-agent-app
git commit -m "feat: quiesce and recover local runtime"
```

### 任务 16：构建生成的 React 数据层和 SSE 状态归约器

**文件：**
- 新建：`web/package.json`
- 新建：`web/package-lock.json`
- 新建：`web/tsconfig.json`
- 新建：`web/tsconfig.app.json`
- 新建：`web/tsconfig.node.json`
- 新建：`web/vite.config.ts`
- 新建：`web/vitest.config.ts`
- 新建：`web/index.html`
- 新建：`web/scripts/generate-api.mjs`
- 新建：`web/src/vite-env.d.ts`
- 新建：`web/src/api/generated/schema.d.ts`
- 新建：`web/src/api/types.ts`
- 新建：`web/src/api/client.ts`
- 新建：`web/src/api/sse.ts`
- 新建：`web/src/state/model.ts`
- 新建：`web/src/state/reducer.ts`
- 新建：`web/src/state/useAgentState.ts`
- 新建：`web/src/test/setup.ts`
- 新建：`web/src/api/client.test.ts`
- 新建：`web/src/api/sse.test.ts`
- 新建：`web/src/state/reducer.test.ts`
- 新建：`web/src/state/useAgentState.test.tsx`

**接口：**
- 产出：生成的 OpenAPI 别名、`ApiClient`、`SseClient`、规范化的 `AgentState`、纯函数 `agentReducer` 和 `useAgentState` 编排逻辑。
- 不变量：任何 TypeScript 文件都不得手写服务器 DTO 结构；别名必须通过生成输出中的 `components["schemas"]` 解析。

- [ ] **步骤 1：锁定前端工具链并生成锁文件**

在 `engines` 中使用 Node 24，并采用以下精确的软件包版本：

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

使用 `npm --prefix web install --package-lock-only` 创建首个锁文件，随后只能通过 `npm --prefix web ci` 按该锁文件安装。提交锁文件；不得使用版本浮动的 `npx` 软件包。

将 `tsconfig.json` 设为构建模式引用，指向 `tsconfig.app.json` 和 `tsconfig.node.json`。二者都使用 `strict`、`noUncheckedIndexedAccess`、`exactOptionalPropertyTypes`、`isolatedModules`、`noEmit` 和打包器模块解析。应用配置以 ES2023 为目标，包含 DOM/DOM.Iterable 和 `react-jsx`；Node 配置为 Vite/Vitest/Playwright 配置文件提供 Node 24 类型。`tsc -b` 必须对二者完成类型检查。

- [ ] **步骤 2：导出 OpenAPI，编写失败的客户端/归约器测试并确认测试为红**

运行：`cargo run -p coding-agent-api --bin export_openapi -- web/openapi.json`

运行：`npm --prefix web run api:generate`

编写测试，证明 URL 片段会在交换请求开始前被清除，启动数据会规范化仓库/任务，重复事件 ID 会被忽略，乱序或非单调的持久化 ID 会强制重新获取启动数据；使用当前模式定义但名称种类未知的事件会记录诊断信息、推进其已验证的持久化游标并继续处理；不受支持的模式定义版本会强制重新获取启动数据。测试流式 SSE 解析器能处理任意 UTF-8 分块边界、CRLF/LF 行尾、注释心跳、多行 `data`，以及客户端构建时尚未知的事件名称。使用伪计时器和注入的抖动，证明重连延迟有上限，重连会携带 `after=<lastAppliedId>`，`401` 会进入 SessionExpired 且不重连，正常 EOF/`503` 会从未变的游标重连；格式错误/超大或在 EOF 处截断的帧以及 `stream.reset`，都会在从启动游标重连前先重新获取启动数据。如果该恢复性启动请求失败，应断言进入明确的不可用/协议错误状态，并以有上限的退避重试启动请求，而不是形成紧密的流重连循环。还要证明 TaskDetail 响应只重放游标之后的缓冲 ID，较慢返回的早期任务详情响应不能覆盖较晚的选择，旧服务世代不能使状态倒退，并且取消操作的乐观状态会在 `503` 或竞争性的终态事件出现时回滚。

运行：`npm --prefix web run test:run -- src/api/client.test.ts src/api/sse.test.ts src/state/reducer.test.ts src/state/useAgentState.test.tsx`

预期：测试失败，因为客户端、归约器和钩子行为尚未实现。

- [ ] **步骤 3：实现确定性的 OpenAPI 到 TypeScript 生成流程**

`generate-api.mjs` 调用锁文件所安装的 `openapi-typescript` 可执行文件处理 `web/openapi.json`，写入同目录临时文件，并将行尾规范化为 LF。普通模式仅在字节不同时替换 `src/api/generated/schema.d.ts`；`--check` 比较临时文件与已提交文件的字节，若不同则以非零状态退出，但不修改文件。`types.ts` 包含如下别名：

```ts
import type { components } from "./generated/schema";

export type Task = components["schemas"]["TaskDto"];
export type TaskDetail = components["schemas"]["TaskDetailDto"];
export type TaskEvent = components["schemas"]["TaskEventDto"];
export type BootstrapResponse = components["schemas"]["BootstrapResponse"];
```

`api:check` 执行该字节比较，因此可以跨平台运行且不会改动工作树。

- [ ] **步骤 4：实现会话/启动数据与类型化 REST 命令**

首次加载时，将 `location.hash` 中的 `token` 解析到内存中，同步调用 `history.replaceState`，保留路径/查询但移除片段，随后且仅在此之后交换令牌。刷新时如果没有片段，则使用仅限主机的 cookie 尝试经过身份验证的启动请求。`ApiClient` 始终使用同源相对 URL、`credentials: "same-origin"`、JSON 解码，并对变更请求使用启动数据中的 CSRF 令牌。它向上层提供稳定的错误码、消息、可重试标志、请求 ID 和详细信息。

每次用户执行创建操作时只生成一次新的客户端请求 UUID，并在网络响应结果不明确时的重试中复用它。绝不使用新 ID 重试变更请求。将 `401` 视为 SessionExpired：停止自动重试变更请求，关闭 SSE，并提示用户从原生应用程序重新打开。将 `503 STORE_BUSY` 作为可重试的用户界面反馈，同时保留同一个变更请求 ID。

- [ ] **步骤 5：实现自定义 SSE 重连和纯事件投影**

`SseClient` 不使用原生 `EventSource`，而是使用带 cookie 的同源 `fetch`、`Accept: text/event-stream`、`redirect: "error"`、`AbortController` 和 `ReadableStream` 读取器。原生 `EventSource` 无法捕获未来新增的所有具名事件，因此无法实现已批准的未知种类回退策略。小型增量解析器使用流式严格模式 `TextDecoder`，接受 CRLF/LF/CR 行尾，以换行符合并重复的 `data:` 行，忽略注释心跳，保留每个具名 `event:` 值，并且只在空行处派发；它能够处理任意字节/分块边界，并限制缓冲帧的字节数。每次退出都先关闭/取消当前读取器，然后严格进入一种恢复类别：`401` 进入 SessionExpired 且永不重连；没有残缺帧的正常 EOF、传输失败、重定向拒绝、`408`、`429` 和 `5xx` 会保留游标，并使用有上限的退避重连；除这些暂时状态以外可观测到的非成功响应、无效内容类型、格式错误的 UTF-8/JSON/事件封装、超大或在 EOF 处截断的帧、ID 不一致以及其他协议违规，都会停止流投影，并要求完整获取启动数据后，新连接才能使用启动游标。在 `redirect: "error"` 下，Fetch 只会把被拒绝的重定向暴露为网络错误，因此实现不会声称可以区分这两种情况。恢复性启动请求失败时，应显示明确的不可用/协议错误状态，并使用有上限的退避重试启动请求，而不是在紧密循环中反复打开同一个错误数据流。

重连采用有上限的指数延迟、注入抖动和 `after=<lastAppliedId>`。客户端先将 JSON 解析为 `unknown`，再通过生成的 OpenAPI 类型收窄为已知变体。对于有效的正数持久化 ID 和受支持的模式定义，未知的具名/数据种类会被记录为已忽略的诊断信息，并在不改变面板的情况下推进游标，从而防止无休止重放；格式错误的事件封装、事件名称与数据种类不一致、非单调持久化 ID、不受支持的模式定义版本以及 `stream.reset` 都会触发完整的启动数据获取。具名任务事件、`service.state` 和 `stream.reset` 全部通过同一个解析器处理，因此未来新增的名称无需预先注册也可被观察到。

纯归约器按 ID 存储仓库/任务，另行存储任务顺序、所选任务详情投影、已应用的全局游标、每个所选任务的实时缓冲区、服务世代和临时命令状态。快照变体会替换计划/差异/测试；活动按稳定条目 ID 去重；生命周期变体会更新任务摘要/时间线。已知模式定义中出现未知种类时，记录诊断信息并继续处理。绝不从 `localStorage` 恢复仓库、任务、面板、游标、会话数据或 CSRF；只有无害的视图偏好可以持久化。

- [ ] **步骤 6：实现 `useAgentState` 的快照/实时数据接合**

获取启动数据后立即启动 SSE，不等待详情。每次选择都会递增请求世代、缓冲其实时事件，并且只在世代仍匹配时接受详情响应。先安装快照，再重放排序后满足 `id > detail.event_cursor` 的缓冲事件；丢弃更旧或相等的事件。非选中任务的全局任务摘要仍持续更新。重新选择任务时，应重新获取详情，而不是信任不完整的浏览器历史记录。

- [ ] **步骤 7：运行前端数据层门禁并提交**

运行：`npm --prefix web run api:check`

运行：`npm --prefix web run typecheck`

运行：`npm --prefix web run test:run`

预期：生成结果无漂移，所有客户端/归约器/钩子竞态测试均通过。

```bash
git add web/package.json web/package-lock.json web/tsconfig.json web/tsconfig.app.json web/tsconfig.node.json web/vite.config.ts web/vitest.config.ts web/index.html web/openapi.json web/scripts web/src/api web/src/state web/src/test web/src/vite-env.d.ts
git commit -m "feat: add typed react data layer"
```

### 任务 17：实现 React 三面板工作台

**文件：**
- 新建：`web/src/components/AppShell.tsx`
- 新建：`web/src/components/Sidebar.tsx`
- 新建：`web/src/components/TaskWorkspace.tsx`
- 新建：`web/src/components/TaskComposer.tsx`
- 新建：`web/src/components/PlanPane.tsx`
- 新建：`web/src/components/ActivityPane.tsx`
- 新建：`web/src/components/ResultPane.tsx`
- 新建：`web/src/components/ConnectionBanner.tsx`
- 新建：`web/src/components/ErrorBoundary.tsx`
- 新建：`web/src/components/AppShell.test.tsx`
- 新建：`web/src/components/TaskWorkspace.test.tsx`
- 新建：`web/src/styles.css`
- 新建：`web/src/main.tsx`

**接口：**
- 使用：仅使用任务 16 的钩子和生成类型。
- 产出：桌面优先的响应式三面板工作台，包含仓库/任务导航、任务创建、取消/重试、面板投影和显式退出应用功能。
- 不变量：Project 1 绝不显示合并、审查通过、可交付或真实代码编辑控件；Completed 只能标记为假执行已完成。

- [ ] **步骤 1：编写失败的交互和无障碍测试**

使用可控的钩子适配器渲染外壳。覆盖仓库/任务选择、空状态、创建验证，Queued、Running、Completed、Failed、Cancelled 和 Interrupted 的操作矩阵，取消进行中禁用操作，创建重试后沿线性重试链导航，较早尝试的只读控件，退出确认，服务横幅，降级关闭警告，缓慢/错误面板以及请求 ID 显示。使用角色/名称查询和键盘用户事件。

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

为地标区域、可见焦点、标签、`aria-live="polite"` 以及不依赖颜色的文本/图标状态提示添加自动化断言。

- [ ] **步骤 2：运行组件测试并确认测试为红**

运行：`npm --prefix web run test:run -- src/components/AppShell.test.tsx src/components/TaskWorkspace.test.tsx`

预期：测试失败，因为工作台组件尚不存在。

- [ ] **步骤 3：实现外壳、侧边栏、编辑器和连接状态**

使用带可见标题的语义化 `header`/`nav`/`main`/`aside` 区域。左侧面板按最近打开时间列出仓库、按创建时间列出任务，提供直接路径注册和原生选择器，并为符合条件的已中断/终态任务提供 Retry，同时让旧尝试保持可选择但只读。它只保存无害的选择/折叠偏好。编辑器会去除提示词首尾空白，显示相对于 50,000 上限的 Unicode 标量值数量，在结果不明确的重试期间持有一个稳定的客户端请求 ID，并将 API 错误与相应字段关联。

页眉显示 Connected、Reconnecting、Store degraded、Shutting down、Session expired 或 Server unavailable。应用菜单中的显式退出操作与浏览器卸载相互独立；不得注册 `beforeunload` 取消或关闭处理器。

- [ ] **步骤 4：实现带独立错误边界的中间面板和结果面板**

中间面板显示任务标题/提示词/尝试/状态、三步计划快照、活动、编辑器和 Running 状态下的取消操作。右侧面板显示合成差异、测试快照、包含结构化失败信息的生命周期时间线，以及可只读导航至较早尝试的线性重试链。所有证据区域都必须提供文本状态和空白/加载/错误状态。分别用错误边界包裹计划/活动/差异/测试/时间线区域，使渲染失败时导航和取消仍可使用。新活动使用单个非打断式实时区域，且绝不移动焦点。

对于 Running 状态下的取消，只在 REST 快照/事件解决该状态或错误将其回滚之前显示本地 Cancelling。旧尝试均为只读，但符合条件的终态可执行 Retry。绝不能从 Completed 推断已经审查或可以合并。

- [ ] **步骤 5：实现响应式布局和视觉状态**

使用 CSS Grid 构建桌面端三列布局，各表面呈现有界且可调整大小的外观，但不持久化领域布局。宽度较窄时，以堆叠/标签页形式保留全部三个语义区域。定义高对比度焦点环、减少动态效果支持、不依赖颜色的状态图形/文本、滚动范围约束，以及易读的差异换行。不得加载远程字体、图像、脚本或样式。

- [ ] **步骤 6：运行用户界面和完整前端门禁**

运行：`npm --prefix web run typecheck`

运行：`npm --prefix web run test:run`

运行：`npm --prefix web run build`

预期：组件/数据测试通过，且 Vite 生成的 `web/dist` 不含外部资源 URL。

- [ ] **步骤 7：提交工作台**

```bash
git add web/src/components web/src/styles.css web/src/main.tsx
git commit -m "feat: add react coding workbench"
```

### 任务 18：嵌入生产 Web 构建并强制实施浏览器策略

**文件：**
- 修改：`crates/coding-agent-app/Cargo.toml`
- 新建：`crates/coding-agent-app/build.rs`
- 新建：`crates/coding-agent-app/src/static_assets.rs`
- 新建：`crates/coding-agent-app/tests/static_assets.rs`
- 修改：`crates/coding-agent-app/src/server.rs`
- 修改：`crates/coding-agent-app/src/main.rs`
- 修改：`web/vite.config.ts`
- 修改：`.gitignore`

**接口：**
- 产出：Cargo 功能特性 `embedded-web` 和 `e2e`、`StaticAssetService`、SPA 回退、确定性的缓存/安全标头，以及发布构建防护。
- 不变量：没有嵌入资源就无法编译发布二进制文件；运行时既不需要 Node，也不需要 `web/dist`。

- [ ] **步骤 1：编写失败的静态资源和标头测试**

构建最小化 Vite 测试夹具，并测试 `/`、一个带哈希的 JS 资源、未知 SPA 路由、带扩展名的未知文件名以及 `/api/not-a-route`。断言 MIME 类型、精确的响应正文字节、HTML/API 使用 `no-store`、带哈希资源使用一年期不可变缓存、缺失资源/API 返回 404，并且只对接受 HTML、无扩展名且非 API 的 GET 请求执行首页回退。

断言每个生产响应都包含 `X-Content-Type-Options: nosniff` 和 `Referrer-Policy: no-referrer`；HTML 必须使用准确的已批准 CSP，只允许 `self`/`data` 来源，且不允许内联内容。断言不存在 CORS 标头。

- [ ] **步骤 2：运行嵌入资源测试并确认测试为红**

运行：`npm --prefix web run build`

运行：`cargo test -p coding-agent-app --test static_assets --features embedded-web`

预期：编译或测试失败，因为嵌入服务/功能特性尚未实现。

- [ ] **步骤 3：定义构建功能特性和发布防护**

声明可选的 `rust-embed` 和以下功能特性依赖边：

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

`build.rs` 为 `../../web/dist` 发出 `rerun-if-changed`。`main.rs` 在 `all(not(debug_assertions), not(feature = "embedded-web"))` 条件下使用 `compile_error!`，因此不可能生成不可用的发布构建。未启用 `embedded-web` 的调试开发模式要求显式使用 Vite 代理；E2E 使用 `e2e`，确保调试资源真正嵌入。

- [ ] **步骤 4：实现嵌入资源查找和安全的 SPA 回退**

针对 `web/dist` 派生 `RustEmbed`，规范化请求路径且不接受反斜杠、点路径段、百分号解码后的路径穿越或 NUL，并优先精确查找资源。仅当 GET/HEAD 请求位于 `/api` 和 `/_local` 之外、不含文件扩展名且接受 HTML 时，才提供 `index.html`。使用 `mime_guess`；HEAD 返回相同标头和空响应正文。

设置 Vite `build.outDir = "dist"`、`emptyOutDir = true` 和 `manifest = true`。从嵌入的 `.vite/manifest.json` 中识别内容哈希文件名，而不是使用宽松的正则表达式；绝不提供该清单或任何以点开头的内部路径。带哈希资源使用 `public,max-age=31536000,immutable`；HTML 和每个 API 响应使用 `no-store`。服务器最外层为成功和错误响应设置 CSP、nosniff 和引用来源策略。

- [ ] **步骤 5：验证开发代理和生产契约顺序**

Vite 只将 `/api` 和 `/_local` 代理到显式提供的 Axum 目标，并保持 SSE 流式传输。后端开发环境的 `public_origin` 是唯一配置的 Vite 源；不存在通配 `localhost` 规则。

按以下顺序运行：

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

预期：所有命令均通过，嵌入服务测试提供构建后的精确字节，并生成受防护的发布可执行文件。任务 20 的真实进程发布冒烟测试负责提供更强的证明：当 `web/dist` 和 Node 均不可用时，该产物能够从干净目录提供 `/`；此处不得仅凭构建证据声称已经获得该运行时结果。

- [ ] **步骤 6：提交生产嵌入实现**

```bash
git add .gitignore crates/coding-agent-app web/vite.config.ts Cargo.lock
git commit -m "feat: embed secured react application"
```

### 任务 19：添加真实进程 Playwright 覆盖和故障注入

**文件：**
- 新建：`crates/coding-agent-app/src/test_support.rs`
- 新建：`crates/coding-agent-app/tests/process_support.rs`
- 新建：`web/playwright.config.ts`
- 新建：`web/e2e/support/localApp.ts`
- 新建：`web/e2e/local-app.spec.ts`
- 修改：`crates/coding-agent-app/src/main.rs`
- 修改：`crates/coding-agent-app/src/store_writer.rs`
- 修改：`crates/coding-agent-app/src/fake_runner.rs`

**接口：**
- 产出：受功能特性控制的进程配置、确定性的假运行器/存储故障脚本，以及用于真实 Axum/SQLite/SSE/React 进程的 Playwright 测试框架。
- 不变量：生产构建中不存在测试 HTTP 路由、提示词魔法字符串、数据目录覆盖、场景解析器或故障注入器。

- [ ] **步骤 1：编写失败的进程支持契约并起草会话 E2E**

在 `#[cfg(feature = "test-support")]` 下，针对 `ProcessTestConfig::load(path)` 编写 `tests/process_support.rs`。使用私有临时应用数据/运行时根目录和场景文件，断言封闭模式定义接受完整的已批准测试夹具、拒绝未知字段、在执行单元启动前验证全部路径、恰好读取一次文件，并且成功加载后源路径不残留任何场景字节。

运行：`cargo test -p coding-agent-app --features test-support --test process_support`

预期：编译失败，因为受功能特性控制的 `test_support` 模块和 `ProcessTestConfig` 尚不存在。此红灯步骤不得启动当前二进制文件：覆盖机制实现之前，它会使用真实用户应用路径。

`localApp.ts` 必须创建私有临时应用数据/运行时根目录和真实临时 Git/Cargo 仓库，启动 `CODING_AGENT_E2E_BINARY` 指定的二进制文件，等待以原子方式发布的描述文件，调用受启动器保护的 `/_local/reopen`，导航到其片段 URL，并保证清理子进程。它只在测试失败时记录 `stdout`/`stderr`，并对描述文件中的秘密信息脱敏。

起草第一个 Playwright 测试，断言在观察到交换请求之前，`location.href` 和浏览器历史记录中均不存在片段；启动请求成功；不存在 CORS 标头；每个页面请求都是同源回环请求或 `data:` URL。它只能在步骤 3 中首次执行，此时进程隔离已经存在且已导出明确的二进制路径。

- [ ] **步骤 2：实现编译时隔离的测试支持**

仅在 Cargo 功能特性 `test-support` 下，接受应用数据、运行时描述文件和单个 JSON 场景文件的环境路径，并在执行单元启动前加载该场景文件。封闭模式定义包含有序的 `FakeScenario` 值、StoreWriter 故障点/次数、执行单元暂停点、虚拟释放信号和标记写入失败。拒绝未知字段，并在构造完成后删除/清零已解析的字节。

生产环境的 `FakeTaskRunner` 仍只产生 Success。生产环境 StoreWriter 不包含故障分支。不得暴露用于更改场景的 HTTP 路由；浏览器唯一可见的接口是常规产品 API。

运行：`cargo test -p coding-agent-app --features test-support --test process_support`

预期：封闭模式定义、单次读取、验证和源字节移除测试均通过。

- [ ] **步骤 3：完成真实进程测试框架**

在 `web/dist` 存在后，使用 `cargo build -p coding-agent-app --features e2e` 构建。测试框架写入场景 JSON，使用受功能特性控制的环境启动一个进程，以有界的重新打开重试读取描述文件，并使用返回的 URL。辅助函数通过可见用户界面创建任务、轮询可访问状态、启动第二个二进制进程、使用同一数据库终止/重启主进程，并且只检查经过授权的 API 响应。

生命周期场景使用单个工作进程运行 Playwright。不得使用 `page.route` 模拟产品 API/SSE；拦截只能在出现非回环出站请求时使测试失败。

运行：`npm --prefix web run build`

运行：`cargo build -p coding-agent-app --features e2e`

运行：`npm --prefix web exec -- playwright install chromium`

PowerShell：`$env:CODING_AGENT_E2E_BINARY=(Resolve-Path target/debug/coding-agent-app.exe)`

PowerShell：`npm --prefix web run e2e -- --grep "clears launch token"`

POSIX：`export CODING_AGENT_E2E_BINARY="$PWD/target/debug/coding-agent-app"`

POSIX：`npm --prefix web run e2e -- --grep "clears launch token"`

预期：首个真实进程测试仅使用隔离路径和明确指定的二进制文件并顺利通过。

- [ ] **步骤 4：添加核心工作流和并发 E2E 场景**

覆盖添加、复用和切换两个真实 Git/Cargo 仓库；比较每个测试夹具在发现前后的文件列表、锁文件字节和脏状态。随后覆盖任务创建，确定性的计划/活动/差异/测试，任务继续运行时刷新以及完整关闭/重新打开页面，4 个 Running 加第五/第六个 Queued，在释放许可前取消第五个从而只有第六个启动，取消 Running，取消/完成竞争时首次提交获胜，失败，`panic` 隔离，重试幂等性，旧尝试只读行为，以及用户界面错误/请求 ID 渲染。分别在已获取许可、已注册临时句柄和已提交 Running 这三个暂停点运行领取/取消场景；任何场景都不得在没有取消令牌时暴露 Running。

针对 cookie、Host、Origin、CSRF、启动器秘密信息缺失或错误，以及启动令牌重放，添加直接的未授权请求矩阵。验证未经身份验证时无法通过 HTTP 端点打开原生选择器。

- [ ] **步骤 5：添加生命周期、恢复和故障 E2E 场景**

覆盖第二个进程在不创建第二个写入器的情况下重新打开第一个进程，在提交后唤醒前、恢复后描述文件生成前，以及描述文件生成后浏览器打开前三个点强制终止，重启时将 Queued/Running 转换为 Interrupted，写入器/事件分发器通知丢失，接收端滞后追赶，以及 stream.reset。对于可恢复的后台终态写入失败，断言用户界面显示 Store degraded，不再开始新的任务领取，恢复操作将状态不明确的任务改为 Interrupted，并且只有持久化事件可见后，横幅才恢复为 Connected。在从 Web 用户界面退出期间，释放屏障周围并发的创建/重试/领取/结果暂停点，并断言不会残留迟到的 Queued/Running 提交，未完成任务为 Interrupted 而非 Cancelled。

对于忽略取消的运行器，从 Web 用户界面调用退出，并断言其 Task 已持久化为 Interrupted，且进程等待不超过 10 秒。对于永久性 Store 故障，从 Web 用户界面调用退出，并断言在 10 秒内以非零状态退出。在一个变体中，断言私有标记仍然存在，恢复写入后，下一次启动会中断未完成任务并移除该标记；在第二个变体中，强制标记创建失败，同时仍断言及时释放锁/描述文件。

对于 TaskDetail/SSE 接合，在详情读取快照后将其暂停，提交实时事件，再恢复执行，并断言面板恰好包含该事件一次。对于服务状态，在获取启动数据和建立 SSE 之间改变世代，并断言首个控制事件可防止状态倒退。

- [ ] **步骤 6：构建并运行完整的真实进程测试套件**

运行：`npm --prefix web run build`

运行：`cargo build -p coding-agent-app --features e2e`

PowerShell：`$env:CODING_AGENT_E2E_BINARY=(Resolve-Path target/debug/coding-agent-app.exe)`

PowerShell：`npm --prefix web run e2e`

POSIX：`export CODING_AGENT_E2E_BINARY="$PWD/target/debug/coding-agent-app"`

POSIX：`npm --prefix web run e2e`

预期：所有测试都驱动真实的嵌入式应用程序；不会出现模拟 API 调用或非回环请求。

- [ ] **步骤 7：提交进程级验证**

```bash
git add crates/coding-agent-app web/playwright.config.ts web/e2e
git commit -m "test: cover local app as a real process"
```

### 任务 20：添加三种操作系统的 CI、发布冒烟测试和运维文档

**文件：**
- 新建：`.github/workflows/ci.yml`
- 新建：`crates/coding-agent-app/tests/release_smoke.rs`
- 新建：`scripts/check-placeholders.mjs`
- 新建：`README.md`
- 修改：`Cargo.lock`
- 修改：`web/package-lock.json`

**接口：**
- 产出：一个 Linux 完整质量/E2E 作业、一个 Windows/macOS/Linux 发布冒烟测试矩阵，以及准确的开发/发布运行手册。
- 不变量：被测试的发布产物从干净目录运行，其子进程无法使用 Node 和 `web/dist`。

- [ ] **步骤 1：编写失败的跨平台发布冒烟测试**

`release_smoke.rs` 读取 `CODING_AGENT_RELEASE_BINARY`，只将该可执行文件复制到干净的临时目录，从子进程的 `PATH` 中移除包含 Node 的条目，将操作系统应用数据/运行时环境变量重定向到临时目录树，并断言无法在同一子进程环境中启动 `node --version`。它在不带 CLI 参数的情况下启动生产产物，等待私有描述文件，断言其端口是随机的 `127.0.0.1` 监听器，使用原始 HTTP/1.1 验证受启动器保护的就绪状态，获取一次性 URL 并进行交换，获取启动数据和 `/`，随后发送受保护的退出请求，并断言干净退出且描述文件已移除。

在专用 `target` 目录中构建使用默认功能特性的调试应用，确保不会意外复用任务 19 的嵌入式 E2E 产物。导出其绝对路径，然后针对这个确定未嵌入资源的二进制文件运行被忽略的冒烟测试：

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

预期：测试执行到生产产物启动/静态资源断言，并因这个明确指定的二进制文件没有嵌入 Web 资源而失败。环境变量缺失、可执行文件缺失或复用 `e2e` 二进制文件，都不是预期的红灯结果。

- [ ] **步骤 2：在不增加第二套运行时依赖的前提下实现发布冒烟测试**

只使用 Rust 标准库的 TCP/进程/文件 API 和现有 serde 类型；不得添加 Node、浏览器、curl、Python 或 TLS 客户端依赖。过滤 Node 目录时保留 PATH 中的操作系统目录，以便浏览器启动可以使用平台机制。冒烟测试允许尝试打开浏览器，但绝不假定浏览器页面会参与流程。子进程退出后移除全部临时数据。

执行 `npm --prefix web run build` 后，运行：

```bash
cargo build --release -p coding-agent-app --features embedded-web --locked
```

PowerShell：`$env:CODING_AGENT_RELEASE_BINARY=(Resolve-Path target/release/coding-agent-app.exe)`

POSIX：`export CODING_AGENT_RELEASE_BINARY="$PWD/target/release/coding-agent-app"`

然后运行：

```bash
cargo test -p coding-agent-app --test release_smoke --features embedded-web -- --ignored --exact release_binary_starts_without_node_or_dist
```

- [ ] **步骤 3：添加确定性的 Linux 质量和 E2E CI**

Linux 作业使用 Node 24 和 `rust-toolchain.toml`，先安装依赖项/浏览器，然后为全部构建/测试门禁导出 `CARGO_NET_OFFLINE=true` 和 `npm_config_offline=true`。严格按以下顺序运行：

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

显式导出 E2E 二进制路径。OpenAPI/TypeScript 生成完成后，只要出现任何已跟踪差异就判定失败。如果页面尝试发出非回环网络请求，Playwright 也会判定失败。

- [ ] **步骤 4：添加 Windows/macOS/Linux 发布冒烟测试矩阵**

矩阵中的每个平台都运行 `npm --prefix web ci`、API 生成检查、类型检查、前端测试/构建、针对受支持平台目标的 Rust `fmt`/`clippy`/测试、生产发布构建以及 `release_smoke.rs`。完整 Chromium E2E 在 Linux 上运行；另外两个平台仍会验证真实监听器、描述文件权限、嵌入式 HTML、会话/启动数据以及干净退出。

该矩阵必须覆盖 Unix 区分大小写的仓库身份、Windows 不区分大小写的身份/DACL 行为、macOS 原生对话框适配器构造、锁/描述文件原子性，以及从仓库外启动产物。不得将任何平台的冒烟测试标记为 `continue-on-error`。

- [ ] **步骤 5：记录直接启动、开发、安全和范围**

README 涵盖前置条件，使用明确配置的公共 Origin 开展 Vite 与 Axum 联合开发，生产构建顺序，直接启动可执行文件，应用数据/运行时位置，如何从 Web 用户界面退出，浏览器打开失败后的恢复，数据库备份，以及稳定的故障排查代码。明确说明 Project 1 是确定性的假平台：它不会读取/修改源代码、调用模型、创建工作树、运行仓库测试、审查或合并，也不表示可交付。安装程序、macOS 应用程序包、Linux 桌面项、签名/公证、自动更新和完善的启动器打包在当时路线图中推迟到 Project 4；按 2026-08-29 范围修订归未来 P4-D。这些都不是 Project 1 的 CI 门禁。

记录威胁边界：回环地址加 Host/Origin/CSRF 可以防御常规跨站访问，但不能防御已由同一操作系统用户运行的恶意进程。记录关闭浏览器不会停止任务或应用程序。

- [ ] **步骤 6：仅保留任务 20 改动时运行最终验证序列**

在本地运行当前平台支持的每条任务 20 CI 命令，并额外运行：

```bash
git diff --check
node scripts/check-placeholders.mjs
git status --short
```

`check-placeholders.mjs` 通过 `git ls-files --cached --others --exclude-standard` 获取已跟踪路径以及未被忽略的未跟踪路径，排除实施计划 Markdown 和自身的标记定义源文件，扫描其余文本文件中的禁用标记，并且仅在没有匹配项时以零状态退出。预期：格式检查、静态检查、全部 Rust/前端/E2E 测试、OpenAPI 漂移检查、嵌入式发布和发布冒烟测试均为绿；`git diff --check` 和标记扫描结果干净；状态只列出任务 20 文件和上述锁文件差异。

- [ ] **步骤 7：提交 CI 和发布文档**

```bash
git add .github/workflows/ci.yml crates/coding-agent-app/tests/release_smoke.rs scripts/check-placeholders.mjs README.md Cargo.lock web/package-lock.json
git commit -m "build: verify local app releases"
```

- [ ] **步骤 8：验证已提交的发布门禁干净**

运行：`git status --short`

预期：无输出。如果仍有生成文件或锁文件改动，重新运行负责生成该文件的门禁，将改动加入任务 20 提交，并在声称完成前重复所有受影响的检查。
