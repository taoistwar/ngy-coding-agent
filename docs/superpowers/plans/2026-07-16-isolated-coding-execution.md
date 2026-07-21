# 隔离式编码执行实施计划

> 执行规则：按照任务顺序，逐项使用 TDD 完成。执行本计划期间，不得开始实现 Project 3 的行为。

**目标：** 将生产环境中的假运行器替换为一个全局并发数为 1 的编码运行器。该运行器为每次尝试创建独立的 Git 工作树，驱动兼容 OpenAI 的单角色工具循环，安全地读取/替换文件，执行有界的类型化 Cargo/Git 操作，并通过现有的 Project 1 生命周期持久化真实可信的计划、活动、差异和测试事件。

**架构：** 新增 `coding-agent-core`，用于定义提供方/运行时端口和确定性的智能体循环；新增 `coding-agent-provider`，用于实现范围锁定的 HTTP 子集；新增 `coding-agent-runtime`，用于提供 Git、文件、进程和 Cargo 能力。`coding-agent-app` 仍是适配器和组合根：它将中立的核心事件映射为 `RunnerEvent`，通过 `StoreWriter` 串行处理尝试产物的变更，并且只有它实现 `TaskRunner`。

**技术栈：** Rust 1.97、2024 版、Tokio、Axum 回环模拟服务器、基于 rustls 的 HTTPS、SHA-256、SQLite/SQLx、Unix 进程组、Windows 作业对象、React/TypeScript/Vite。

## 全局约束

- 源规范：`docs/superpowers/specs/2026-07-16-isolated-coding-execution-design.md`。
- 使用 `codex/` 实现分支，并保留用户已有的、与本任务无关的改动。
- 对每项行为：先添加聚焦的失败测试，确认出现预期失败，再添加最小实现，重新运行聚焦测试及受影响的测试套件，最后检查差异。
- 依赖图必须无环：`app -> {core,provider,runtime}` 和 `{provider,runtime} -> core`；核心软件包永远不得依赖应用软件包。
- 运行时 SQLite 变更必须通过 `StoreWriter`；运行器绝不直接写入 `Store`。
- 第一方文件目标和命令当前工作目录（`cwd`）必须位于工作树内。Cargo 执行的原始代码和模型生成代码均视为可信代码，并具有当前用户权限；不得声称存在操作系统级沙箱，并且必须在产品用户界面/文档中展示此警告。
- 隐藏并保护全部 `.git` 元数据。不得运行 Git 钩子以及过滤器/配置中指定的可执行命令。
- 模型只能接收类型化工具；它永远不能提供任意可执行文件、参数向量（`argv`）、当前工作目录（`cwd`）、Git 路径选项、Cargo 清单/目标/配置路径或命令解释器字符串。
- 生产环境真实运行器的并发数必须恰好为 1。测试环境假运行器的并发数可以保持为 4；启动阶段必须报告实际选择。
- 提供方、命令、差异和工具输出必须先经过有界处理和脱敏，再进入跟踪日志、SQLite 或模型上下文。
- 默认测试必须离线，且只能使用回环模拟 HTTP。
- `Completed` 仍然只表示运行器成功，绝不表示已经审查、可交付或可合并。

## 锁定的归属映射

```text
Cargo.toml / Cargo.lock
crates/coding-agent-core/
  src/{lib,error,limits,model,ports,agent,tools,conversation,budget,event}.rs
  tests/{ports,agent_loop,budgets,cancellation}.rs
crates/coding-agent-provider/
  src/{lib,config,dto,error,redaction,client}.rs
  tests/{schema,redaction,contract}.rs
crates/coding-agent-runtime/
  src/{lib,relative_path,root_capability,files,search,replace}.rs
  src/{command_policy,environment,output,cargo,diff,runtime_adapter}.rs
  src/git/{mod,worktree}.rs
  src/process/{mod,unix,windows}.rs
  tests/* plus tests/support/mod.rs
crates/coding-agent-store/
  migrations/0002_task_attempt_artifacts.sql
  src/attempt_artifacts.rs
  tests/attempt_artifacts.rs
crates/coding-agent-app/
  src/{coding_agent_runner,provider_config}.rs
  src/{store_writer,single_instance,main,test_support,lib}.rs
  tests/{coding_agent_runner,provider_config,isolated_coding_e2e}.rs
web/src/ and README.md
```

如果采用更小的模块会更清晰，可以合并命名，但归属和依赖方向必须保持锁定。

## 任务 1：建立核心端口和软件包依赖图

- [x] 添加失败的 `coding-agent-core/tests/ports.rs` 契约，覆盖提供方消息、恰好一次工具调用、异步 `ModelProvider`/`ToolRuntime` 端口、取消、中立的运行器事件，以及经过验证且非零的 `AgentLimits`。
- [x] 运行 `cargo test -p coding-agent-core --test ports`，确认因软件包/API 缺失而失败。
- [x] 向工作区添加三个软件包和最小化的核心 DTO/特征实现。提供方/运行时软件包的根模块仅编译其预期的依赖边。
- [x] 验证：

```powershell
cargo test -p coding-agent-core --test ports
cargo check --workspace --all-targets
cargo fmt --all --check
```

检查点：任何新软件包都不得依赖 `coding-agent-app`。

## 任务 2：解析具备能力安全性的相对路径

- [x] 添加失败的运行时测试，覆盖绝对路径、UNC 路径、设备路径、`.`/`..`、NUL、Windows 备用数据流（ADS）、平台等价的 `.git` 名称、非 UTF-8 输入、符号链接/目录联接/重解析点穿越，以及祖先目录交换竞态。
- [x] 将 `RelativePath` 实现为不携带访问权限的逻辑路径。
- [x] 使用基于句柄/文件描述符、逐组件且不跟随链接的相对遍历来实现 `RootCapability`。如果平台无法建立所需语义，写入操作必须安全拒绝。
- [x] 将平台相关的不安全代码限制在最小范围内，并记录其不变量。
- [x] 运行 `cargo test -p coding-agent-runtime --test path_security -- --nocapture`，并对该软件包运行 `Clippy` 检查。

## 任务 3：实现有界的读取/列出/搜索

- [x] 测试行范围、字节截断、二进制拒绝、深度/条目上限、字面量搜索/通配模式验证、受保护的 `.git`、默认排除 `target`，以及并发替换祖先目录。
- [x] 实现基于句柄的相对路径工具，不依赖外部 `rg`。
- [x] 返回结构化的截断/计数元数据和稳定错误码。
- [x] 运行 `cargo test -p coding-agent-runtime --test file_tools` 以及 `path_security`。

## 任务 4：实现与摘要绑定的原子替换

- [x] 测试使用 `expected_sha256=null` 创建文件、现有文件摘要匹配、摘要过期/目标缺失冲突、权限、同目录独占临时文件、刷盘、发布前取消清理、祖先目录竞态，以及 Windows 目标被占用时的保留行为。
- [x] 实现基于根目录能力的相对临时文件创建、`sync_all`、发布前重新验证和平台原子发布。
- [x] 绝不降级为原地截断或复制/删除；即使发布后立即观察到取消，发布成功也必须视为已提交。
- [x] 运行 `cargo test -p coding-agent-runtime --test atomic_replace -- --nocapture` 以及路径测试。

## 任务 5：监管有界进程树

- [x] 测试分别排空 `stdout`/`stderr`、首尾截断、双管道输出洪泛、非零退出、按实际经过时间计算的超时、取消优先级、主进程退出但孙进程仍存活、异步任务中止/丢弃，以及有界清理。
- [x] 使用明确的平台/工具链环境构造器实现 `env_clear()`，并证明提供方、代理、SSH 和 CI 的秘密信息不会保留。
- [x] Unix：使用独立进程组并执行终止/等待。Windows：使用作业列表属性，或者暂停创建、分配后恢复，在用户代码执行前将进程分配给带 `KILL_ON_JOB_CLOSE` 的作业对象。
- [x] 确保主进程正常退出后，仍会在有界管道处理完成前清理后代进程；RAII 析构同样会终止进程。
- [x] 在当前平台运行 `bounded_output`、`environment` 和 `process_tree` 测试。

## 任务 6：锁定类型化命令策略和 Cargo 适配器

- [x] 测试只有内部 `ValidatedCommand` 构造器可用，并拒绝命令解释器、任意可执行文件/`argv`/`cwd`、Git 变更/路径选项，以及 Cargo 清单/目标/配置注入。
- [x] 测试固定的工具发现流程，以及离线 `metadata`/`check`/`test` 操作；包/测试名称仅限可信元数据，状态必须真实，并覆盖超时和取消。
- [x] 只实现类型化模型工具：Cargo 的 `metadata`/`check`/`test` 以及 Git 的 `status`/`diff`；不提供通用 `run_command`。
- [x] 运行 `command_policy` 和 `cargo_tools` 测试。

## 任务 7：配置并验证 Git 工作树

- [x] 使用临时真实仓库测试尚无提交的 `HEAD`、脏工作区/已暂存/未跟踪内容的隔离、嵌套工作区映射、唯一分支/路径、重试隔离、冲突，以及创建流程的所有崩溃点。
- [x] 证明 `post-checkout` 钩子、可执行过滤器、`fsmonitor`、外部差异工具和 `textconv` 永远不会执行；必要时拒绝不安全的仓库配置。
- [x] 基于已提交的 `HEAD` 创建工作树，使用确定性的、由应用程序持有的身份，并将后续 Git 操作绑定到已验证的 `git-dir`/`work-tree` 值。
- [x] 永远不要从对模型隐藏的关联工作树 `.git` 文件中派生访问权限。
- [x] 运行 `cargo test -p coding-agent-runtime --test worktree -- --nocapture`。

## 任务 8：收集有界差异

- [x] 测试新增/修改/删除文件、二进制和非 UTF-8 路径、计数、确定性顺序、补丁大小上限，以及禁用外部差异工具和 `textconv`。
- [x] 实现与领域无关的差异 DTO 和具体的运行时适配器。
- [x] 运行 `cargo test -p coding-agent-runtime --test diff` 以及完整的运行时软件包测试。聚焦的差异测试通过；受管控的 Windows 完整软件包测试通过 57/60 个单元测试，另外三个预先存在的 `atomic_replace` 测试因文件系统 `PermissionDenied` 而被阻塞。

## 任务 9：通过 `StoreWriter` 持久化产物生命周期

- [x] 添加 v2 迁移测试，覆盖旧数据库升级、重复迁移、精确约束和回滚。
- [x] 测试任务/仓库/尝试身份、唯一分支/路径、`reserved -> ready|inconsistent`、相同内容的幂等性和冲突拒绝。添加组合数据库身份约束。
- [x] 使用 `reserve`/`ready`/`inconsistent` 操作扩展 `StoreWriter`；不得暴露运行器直接写入能力。
- [x] 测试启动时的一致性校正，覆盖 `reserved+absent`、`reserved+valid`、部分 Git/磁盘状态以及不匹配的 Git/磁盘状态，并区分同一次运行的重新进入与重启后已放弃的状态。
- [x] 运行存储迁移/产物测试和应用 `StoreWriter` 测试。

## 任务 10：验证提供方配置、模式、错误和脱敏

- [x] 测试严格的 `provider.json` 模式、私有权限、远程 URL 仅允许 HTTPS、测试专用回环 HTTP、禁止 `userinfo`/`query`/`fragment`，以及不会泄露秘密的 `Debug`/`Display`。
- [x] 测试 `messages`、单个 `tool_call_id` 往返、多次调用拒绝、未知/超大响应拒绝，以及可重试错误映射。
- [x] 在任何日志或用户边界之前实现脱敏。
- [x] 运行提供方模式/脱敏测试和应用的提供方配置测试。

## 任务 11：实现 HTTPS 提供方契约

- [x] 使用本地 Axum 服务器测试精确的 POST 路径、正文、`tools`、`tool_choice` 和 Bearer 认证行为、超时、401/429/5xx、断开连接、格式错误的正文、缺少 `Content-Length` 的分块数据洪泛、超大 JSON、压缩炸弹、拒绝 30x 重定向，以及请求 ID。
- [x] 添加基于 rustls 的客户端；生产环境 HTTPS 不能依赖可能缺失的原生 TLS 配置。
- [x] 断言任何测试都不会联系未配置或真实的提供方。
- [x] 运行 `cargo test -p coding-agent-provider --test contract -- --nocapture` 以及提供方软件包测试。

## 任务 12：实现确定性的单角色循环

- [x] 使用脚本化端口测试工具调用 → 结果 → 续接 → 最终文本、无效调用、可重试/致命错误、预算、取消优先级和终态快照收集。
- [x] 测试工作区修订版和指纹：必须包含已跟踪内容以及未被忽略的未跟踪内容，排除已忽略的 `target/` 输出；哈希计算必须流式且确定；每项数量/字节上限超限都必须安全拒绝；替换操作会递增修订号并将失效加入队列；开始/结束/最终指纹必须与测试绑定；测试代码或外部进程修改源文件会使通过结果失效；只有当前指纹才允许成功。
- [x] 保持上下文有界，且永不持久化思维链或提供方原始响应正文。
- [x] 只发出中立事件；由应用执行领域映射。
- [x] 运行核心智能体循环、预算和取消测试。

## 任务 13：适配 `CodingAgentRunner`

- [x] 测试预留/配置/就绪、初始计划/活动、事件映射、去抖/终态差异、运行中/终态测试、正常取消、事件接收端拒绝、稳定的失败映射和保留策略。
- [x] 添加强制静默回归测试：`Interrupted` 可以保留最新的已持久化差异，且后续迟到事件仍会被拒绝。
- [x] 使用由 `StoreWriter` 支持的产物和现有 `RunContext` 取消机制实现应用适配器。
- [x] 终态任务转换只能由 `TaskManager` 通过 `RunnerOutcome` 完成。
- [x] 运行编码运行器、任务管理器和关闭流程测试。

## 任务 14：取得主实例锁后选择生产运行器和并发数

- [x] 测试辅助实例启动时忽略提供方配置、主实例要求有效的私有配置、无效/缺失配置绝不回退到假运行器、真实运行器报告并发数 1，以及注入的假运行器报告其配置的并发数。
- [x] 将固定的启动运行器替换为仅在私有路径和主实例锁建立后调用的工厂；返回 `{ runner, concurrency }`。
- [x] 假实现/模拟实现仅在测试中显式使用；使用有效但不被访问的配置更新发布冒烟测试，并断言并发数为 1。
- [x] 运行单实例、进程支持和发布冒烟测试。

## 任务 15：验证离线 E2E 并更新产品界面

- [x] 针对临时的有改动 Rust 仓库编排回环提供方：读取 → 替换 → Cargo 测试 → 最终输出。
- [x] 断言 `Completed`、产物/分支/工作树身份、原始已暂存/未暂存/未跟踪字节不变、当前修订版通过、有界差异、SQLite 投影和 SSE 重放。
- [x] 添加连接断开、测试失败、超时、取消、输出洪泛、测试通过后替换、路径逃逸和重启中断的失败 E2E。
- [x] 更新用户界面文案、README 中的配置/威胁模型/产物/故障排查文档，以及需要明确平台门禁覆盖的 CI。
- [x] 运行前端检查、完整格式检查、`Clippy`、工作区测试和 `git diff --check`。

## 最终审查与验收

- [x] 独立审查重点关注路径竞态、`.git`/Git 配置执行、Windows 执行前作业对象分配、`StoreWriter` 顺序、脱敏和与修订版绑定的测试。
- [x] 解决所有阻断级/高严重度发现，并重新运行受影响的测试。
- [x] 收集最新的三平台 CI 证据。
- [x] 演示成功路径，以及超时、取消、路径逃逸、恶意 Git 配置和测试通过后替换的失败路径。
- [x] 记录准确的命令/结果，并确认不存在秘密信息、生成内容漂移或占位符。

最新的三平台证据来自 GitHub Actions 运行
[`29738805404`](https://github.com/taoistwar/ngy-coding-agent/actions/runs/29738805404)，
对应提交 `f0abaa24e50e9583a27227a6457f7f180b323c00`：

- Linux quality and E2E job `88340446147` — 成功，包括浏览器 E2E 和嵌入式发布构建。
- Ubuntu release-smoke job `88340446186` — 成功。
- Windows release-smoke job `88340446187` — 成功。
- macOS release-smoke job `88340446212` — 成功，包括用于验证 Darwin 进程树清理和并发工作树测试夹具的完整工作区测试。

### 最新验收证据 — Windows 和 GitHub CI，2026-07-20

- `cargo fmt --all -- --check` 和 `git diff --check` — 通过。
- `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` — 通过。
- `cargo test --workspace --all-features --locked` — 最终生产修复完成后，本地测试在 514.7 秒内通过；最新 CI 在 Linux、Ubuntu、Windows 和 macOS 上再次运行了完整工作区测试。
- `cargo test -p coding-agent-app --test task_manager --all-features --locked --offline` — 27/27 通过；FIFO 任务领取顺序回归测试随后连续运行 50/50 次通过，同时观测了运行器实际的启动顺序。
- 聚焦的运行时集成测试（工作树、差异、指纹、类型化 Git 和类型化 Cargo）— 23/23 通过；应用产物一致性校正和真实离线 E2E — 8/8 通过。
- `cargo test -p coding-agent-runtime -p coding-agent-app --lib --all-features --locked --offline` — 151/151 通过，其中包括并发数为 1 的生产运行器工厂和进程监管器清理测试。
- CI 作业还通过了生成 API 漂移检查、前端类型检查/测试/构建、占位符拒绝、嵌入式发布构建和发布应用启动冒烟测试，并且没有联系真实提供方。
- 独立审计已认可 Darwin/XNU 进程组静止状态处理、实际 FIFO 启动顺序同步和进程级全局创建锁覆盖；不再存在阻断级或高严重度问题。
