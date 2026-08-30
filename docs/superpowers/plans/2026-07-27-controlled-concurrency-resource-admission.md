# Project 4A：受控并发与资源准入实施计划

> 日期：2026-07-27
> 状态：已于 2026-08-03 按计划完成 TDD、独立审查与完整门禁
> 执行规则：按任务顺序手工执行 TDD；执行期间不得调用或使用 `superpowers` 技能
> 后续范围修订：已批准的 Project 4 = P4-A + P4-B；P4-C（历史/构件生命周期）与 P4-D（发行/provider 加固）是未来项目

**目标：** 在不改变 Project 3 质量证据、六态生命周期和单 Task 隔离边界的前提下，把生产真实任务提升为默认全局并发 `2`、同 common Git directory 并发 `2`，并补齐确定性的排队、公平调度、队列背压、仓库控制、磁盘准入、停止意图、进程清理、恢复和 Bootstrap/SSE/React 投影。

**源规格：** `docs/superpowers/specs/2026-07-27-controlled-concurrency-resource-admission-design.md`，已于 2026-07-27 获用户书面批准。规格与本计划冲突时以规格为准，停止实现并修订计划，不能现场发明新行为。

**架构：** 依赖方向继续锁定为 `app -> {api,store,core,provider,runtime}`、`{provider,runtime} -> core -> domain`。动态调度、permit、repository control、storage 和 stop-in-progress 归 `app`；SQLite v4、queue-cap、stop-intent 和恢复事务归 `store`；平台 Git/volume/process capability 归 `runtime`；wire DTO 归 `api`；React 只消费服务端 typed projection。

**技术栈：** Rust 1.97、edition 2024、Tokio、Serde、SQLx/SQLite、Axum/OpenAPI、React/TypeScript/Vite/Vitest/Playwright。默认验证全部离线，不联系真实 provider。

## 全局执行约束

- 本目录沿用历史文档组织方式，不表示仓库依赖或启用了 `superpowers` 工作流。
- 每个任务严格执行 red -> green -> refactor：先添加聚焦失败测试并确认失败来自缺失行为，再做最小完整实现，最后运行聚焦测试和受影响回归。
- 每个任务结束都检查 `git status --short`、聚焦 diff 和 `git diff --check`；不覆盖、回退或整理用户无关改动。
- 默认只用 scripted provider、临时 SQLite、临时真实 Git/Cargo 仓库和 injected platform fakes。任何真实 provider 请求都需要新的、限定次数的用户明确授权；本计划不包含此授权。
- 生产期 SQLite mutation 只能通过唯一 `StoreWriter`。migration、startup artifact reconciliation 和 cold recovery 是 single-instance lock 内、dispatcher/TaskManager/Web Ready 之前唯一允许的 direct Store 例外。
- `TaskStatus` 保持六态；11 种持久 task event 继续使用 `schema_version=1`。queue reason、permit、lease、storage sample、Cargo jobs 和 scheduler control 不进入 Task row 或持久 event。
- `Completed + ReviewApproved`、`Failed + ReviewRejected`、generation/digest/fingerprint、diff coverage 和 current-check evidence 的 Project 3 原子约束不得放宽。
- mailbox handler 不等待可能阻塞的普通 StoreWriter future。每 Task mutation sequence、typed pending 和 completion message 必须保留 actor 响应安全通道的能力。
- claim、stop intent、review/finalization、终态和 process cleanup 的 unknown outcome 不能压扁为普通 conflict、closed 或 timeout；必须按 typed disposition 校正。
- repository control lease 不跨正常 Planner/Executor/Reviewer 角色循环；SQLite transaction 不跨 runtime Git side effect；runtime/Store callback 不反向获取 repository lease。
- 已启动进程树未确认退出前不得释放 active permit、写假安全终态或让替代任务启动。cleanup ownership 未完成时 primary 继续持有 single-instance lock。
- 不增加自动 merge、rebase、push、PR、worktree/branch cleanup、history/search、artifact lifecycle、dynamic settings、OS sandbox、provider pool 或发行行为。
- 不自动删除、移动或“修复”用户 Git/worktree/artifact。不可观察和正向不一致证据必须严格区分。
- 所有跨层 wire schema 变更在同一任务中同步 Rust DTO、OpenAPI、`web/openapi.json`、generated TypeScript、runtime validator、reducer 和 fixtures，不能留下半升级 checkpoint。
- 新错误和诊断必须稳定、短且脱敏；不得记录 secret、prompt、diff、reasoning、raw provider body、完整命令输出或绝对路径。
- Windows 上长 Rust/SQLite 测试若因共享 build/数据库锁竞争失败，先确认错误性质，再串行重跑相关命令并等待最终 exit code；不能从静默输出推断通过。

## 锁定的归属映射

```text
crates/coding-agent-store/
  migrations/0004_concurrent_scheduler.sql
  src/{migrate,tasks,claims,reviews,projection,stop_intents,recovery,lib}.rs
  tests/{migrations,tasks,queue_capacity,claims,reviews,projection,stop_intents,recovery}.rs

crates/coding-agent-runtime/
  src/{cargo_tools,command_policy,root_capability,worktree,storage,process_supervisor,
       process_liveness,lib}.rs
  tests/{cargo_tools,path_security,worktree,storage,process_liveness}.rs

crates/coding-agent-app/
  src/{runtime_config,scheduler,repository_control,run_context,storage_policy,storage_monitor,
       coding_agent_runner,runner_factory,artifact_reconciliation,store_writer,
       pending_durable,task_manager,event_dispatcher,bootstrap_join,single_instance,shutdown,
       server,test_support,lib}.rs
  tests/{runtime_config,scheduler,repository_control,storage_policy,storage_monitor,coding_agent_runner,
         artifact_reconciliation,store_writer,task_manager,degraded_recovery,
         single_instance,shutdown,server,offline_e2e,concurrent_offline_e2e}.rs

crates/coding-agent-api/
  src/{contract,backend,router,scheduler_wire,sse,error}.rs
  tests/{openapi,router,sse}.rs

web/
  openapi.json
  src/api/{types,validation,schedulerSnapshot,sse,client}.ts and generated/schema.d.ts
  src/state/{model,reducer,useAgentState}.ts
  src/components/{AppShell,Sidebar,TaskComposer,TaskWorkspace,SchedulerSummary}.tsx
  src/styles.css and adjacent tests
  e2e/{concurrent-scheduler,fault-recovery,lifecycle,ui-edge-cases}.spec.ts

testdata/scheduler-state-rfc8785.json
README.md
```

可以采用更小的同归属模块，但不能反转 crate 依赖、把持久事实放到 app 临时字符串中，或把平台 capability 放进 API/domain。

## Checkpoint A：可信配置与持久化原语

## 任务 1：实现 RuntimeConfig exact loader 与 Cargo jobs 纯计算

**文件：**

- 新建 `crates/coding-agent-app/src/runtime_config.rs`
- 新建 `crates/coding-agent-app/tests/runtime_config.rs`
- 修改 `crates/coding-agent-app/src/{lib,platform,single_instance,test_support}.rs`

- [x] RED：先覆盖 `runtime.json` 缺失时的完整默认值、exact object、缺字段/未知字段/重复 key/尾随内容、错误版本和错误类型；覆盖 global/per-repository/queue 上下界、per-repository 不得大于 global、两个非零 `u64` 以及 checked 乘加溢出。
- [x] RED：复用私有配置测试夹具，证明 symlink/reparse-point、非普通文件、文件过大、不可读和 owner-only 权限失败得到稳定 `RUNTIME_CONFIG_INVALID`，存在但无效时绝不回退默认。
- [x] RED：以注入的 available parallelism `1/2/4/8/64/失败` 和 global limit `1..=4` 锁定 `max(1,min(8,cpu/global))`，并证明进程生命周期内结果稳定。
- [x] GREEN：实现 deny-unknown、拒绝重复 key 的手写反序列化边界、validated newtypes、默认配置和 checked 派生值；Bootstrap 之外不暴露文件内容或路径。
- [x] GREEN：在 primary 启动的 SQLite open 之前加载配置并形成不可变 startup context；此任务只接入读取和传递，不提前扩大 TaskManager 并发。
- [x] REFACTOR：确保 provider/runtime 两份私有配置共享文件安全原语但不共享 schema、错误码或 secret 生命周期。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test runtime_config --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test platform --features test-support --locked --offline
cargo check -p coding-agent-app --all-targets --all-features --locked --offline
```

检查点：只有文件缺失使用默认；文件存在但无法验证时 fail closed。

## 任务 2：把稳定 Cargo jobs 注入全部第一方 check/test

**文件：**

- 修改 `crates/coding-agent-runtime/src/{cargo_tools,command_policy,quality_runtime,runtime_session,lib}.rs`
- 修改 `crates/coding-agent-runtime/tests/{cargo_tools,quality_runtime}.rs`
- 修改 `crates/coding-agent-app/src/{runner_factory,coding_agent_runner,single_instance,test_support}.rs`
- 修改 `crates/coding-agent-app/tests/coding_agent_runner.rs`

- [x] RED：锁定所有第一方 `cargo check`、`cargo test` argv 都含唯一受信 `--jobs=<N>`；metadata/Git/文件工具不错误注入。
- [x] RED：证明模型 selector、required action、额外参数或环境不能提供、覆盖或删除 jobs；不设置 `RUST_TEST_THREADS`，不同 Task 仍各自串行执行工具批次。
- [x] GREEN：让 `CargoTools`/quality runtime 只从 validated `NonZeroU32` startup value 构造受信参数，并通过 RunnerFactory 显式传递；删除任何依赖瞬时 active 数或开发机测试线程的计算。
- [x] REFACTOR：集中 argv 构造，使 check/test 两条生产路径复用同一防重复断言。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test cargo_tools --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test quality_runtime --locked --offline
cargo test -p coding-agent-runtime command_policy --lib --locked --offline -- --nocapture
cargo test -p coding-agent-app --test coding_agent_runner --features test-support --locked --offline
```

检查点：运行中改变配置文件或 active 数不会改变既有 `cargo_jobs_per_task`。

## 任务 3：增加 SQLite v4 与 migration-history fail-closed 校验

**文件：**

- 新建 `crates/coding-agent-store/migrations/0004_concurrent_scheduler.sql`
- 修改 `crates/coding-agent-store/src/{migrate,lib}.rs`
- 修改 `crates/coding-agent-store/tests/migrations.rs`

- [x] RED：先覆盖空库、真实 v1/v2/v3→v4、v4 重开、重复 migrate、逐 SQL 故障回滚和 `PRAGMA foreign_key_check`。
- [x] RED：构造未来版本、0/负版本、缺 1、内部 gap、重复/非法历史，证明以 `DATABASE_SCHEMA_UNSUPPORTED` 在任何新 schema write 前失败。
- [x] RED：覆盖 `task_stop_intents` STRICT/type/CHECK/FK、正 attempt、两个 exact kind、immutable UPDATE/DELETE、显式 duplicate trigger、`INSERT OR REPLACE` 和 UPDATE-upsert 绕过；覆盖 queued partial index。
- [x] RED：直接用raw SQL证明跨表triggers：intent INSERT只允许exact `Running + Unreviewed`；user/disk intent存在时只允许各自规定的Cancelled/Failed终态tuple；intent存在时raw review evidence、delivery/finalization和错误terminal mapping全部abort。
- [x] GREEN：迁移新增 immutable intent 表、跨表状态矩阵triggers和 `tasks(created_at,id) WHERE status='queued'`，不回填 intent、不重写旧 Task/event/artifact/review/readiness。
- [x] GREEN：migration runner 先验证从 1 开始、无空洞且最大不超过 4 的连续前缀，再在单次 open transaction 中依次升级。
- [x] REFACTOR：把数据库 migration version `4` 与 wire/event schema `1` 的命名分开，错误不泄露数据库路径。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test migrations --locked --offline -- --nocapture
cargo test -p coding-agent-store --test projection --locked --offline
```

检查点：11 种持久事件和 Project 3 quality rows 的字节语义不变。

## 任务 4：实现 create/retry 的原子 queue cap 与幂等优先

**文件：**

- 修改 `crates/coding-agent-store/src/{tasks,projection,lib}.rs`
- 新建 `crates/coding-agent-store/tests/queue_capacity.rs`
- 修改 `crates/coding-agent-store/tests/{tasks,projection,support/mod}.rs`

- [x] RED：create 在同一 `BEGIN IMMEDIATE` 中按 Existing-same-input、idempotency conflict、新请求 count、queue-full、insert 的固定顺序执行；满队列 Existing 仍成功，conflict 仍为 409 语义。
- [x] RED：retry 先验证 source 和 direct child；已有 child 在满队列仍 Existing，不可 retry source 优先报原错误，首次 child 才占 slot。
- [x] RED：并发 create/retry 恰好填满最后一个 slot且不超卖；queue-full 零 Task/event/last_event_id 副作用，Running/terminal 不计数。
- [x] RED：legacy `queued > max` 时容量 saturating 为 0、旧行不改写，排空到阈值以下前新请求一直 full。
- [x] GREEN：增加显式 queue-limited typed Store operations 和 `QueueFull { queued_tasks, max_queued_tasks }` disposition；计数、lookup 和 insert 不依赖 API 预查或 StoreWriter 串行性。
- [x] REFACTOR：保留测试/迁移所需最低层 helper；在任务 16 切换生产调用后禁止任何生产入口绕过 queue cap。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test tasks --locked --offline -- --nocapture
cargo test -p coding-agent-store --test queue_capacity --locked --offline -- --nocapture
cargo test -p coding-agent-store --test projection --locked --offline
```

检查点：queue cap 只统计 `Queued`，不引入 per-repository queue quota。

## 任务 5：实现 typed claim 事务与精确校正 receipt

**文件：**

- 新建 `crates/coding-agent-store/src/claims.rs`
- 新建 `crates/coding-agent-store/tests/claims.rs`
- 修改 `crates/coding-agent-store/src/{tasks,lib}.rs`

- [x] RED：首次 claim 精确绑定 task/repository/attempt/原 queued cursor，并原子提交 `Queued -> Running + task.started`、`started_at` 和 `last_event_id`。
- [x] RED：同一 canonical claim 重放返回 `ExistingApplied` 和原 started event ID；并发 claim 最多产生一个 started event。
- [x] RED：权威 Task 仍 Queued或已终态且不存在匹配 started tuple时返回 `KnownNotApplied`；`last_event_id`、event kind/payload、started_at、identity/attempt 任一 partial或错配均为 invariant conflict。
- [x] GREEN：实现 `ClaimTaskRequest` 和 Store-level `Applied|ExistingApplied|KnownNotApplied|InvariantConflict`；`OutcomeUnknown` 只由任务 16 的投递/连接层产生。
- [x] GREEN：封死生产通过通用 `transition_with_event(...Running)` claim 的路径；保留其他合法 lifecycle transition。
- [x] REFACTOR：导出只读 exact claim reconciliation query，供 unknown-outcome adoption 使用，不让 app重新拼事件 tuple。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test claims --locked --offline -- --nocapture
cargo test -p coding-agent-store --test lifecycle --locked --offline
```

检查点：Store只报告数据库内可证明的结果，不推断投递层 outcome certainty。

## 任务 6：实现 immutable stop intent、urgent batch 与 final-stop 事务

**文件：**

- 新建 `crates/coding-agent-store/src/stop_intents.rs`
- 新建 `crates/coding-agent-store/tests/stop_intents.rs`
- 修改 `crates/coding-agent-store/src/{tasks,reviews,projection,lib}.rs`
- 修改 `crates/coding-agent-store/tests/{reviews,lifecycle,projection}.rs`

- [x] RED：单 intent query-first 插入覆盖 Applied、same-kind Existing并返回原`requested_at`、other-kind IntentConflict、terminal won、identity/attempt mismatch 和 raw corruption。
- [x] RED：urgent batch 最多四项、canonical task UUID 顺序、单个 `BEGIN IMMEDIATE`、逐项 `Applied|Existing|TerminalWon|IntentConflict`，数据库级任一点失败整批回滚。
- [x] RED：stop intent、`record_review`、`finalize_reviewed_task` 双向互斥；先进入不可中断 transaction 的一方获胜，不能产生 readiness/terminal 双写。
- [x] RED：`finalize_stopped_task` 对user生成Cancelled、对disk生成`Failed + DISK_PRESSURE_CRITICAL + retryable=true`，并锁定同timestamp、唯一lifecycle event和`last_event_id`；commit-before-wake/reply重放返回原event ID，partial/额外terminal event fail closed。
- [x] GREEN：实现 typed `StopIntentKind`、immutable row codec、batch receipt 和共享 query-first final-stop helper；Queued cancel 继续走既有原子 Cancelled 事务且不建 intent。
- [x] GREEN：在 tasks/reviews 的所有通用终态和质量写入口增加 intent invariant，禁止 `INSERT OR REPLACE`、REPLACE 和隐式修复。
- [x] REFACTOR：让 cold recovery 能复用 transaction-scoped final-stop helper而不嵌套 transaction。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test stop_intents --locked --offline -- --nocapture
cargo test -p coding-agent-store --test reviews --locked --offline -- --nocapture
cargo test -p coding-agent-store --test lifecycle --locked --offline
cargo test -p coding-agent-store --test projection --locked --offline
```

检查点：intent 行在终态后保留；停止不创建第 12 种 event 或 ReviewRejected。

## 任务 7：实现原子 cold recovery 与分场景 shutdown primitives

**文件：**

- 新建 `crates/coding-agent-store/src/recovery.rs`
- 新建 `crates/coding-agent-store/tests/recovery.rs`
- 修改 `crates/coding-agent-store/src/{tasks,projection,lib}.rs`
- 修改 `crates/coding-agent-store/tests/{tasks,projection}.rs`

- [x] RED：一个 `recover_after_restart BEGIN IMMEDIATE` 先验证全部 intent/terminal tuple，再按 `(requested_at,task_id)` 完成 Running intent，按 `(created_at,task_id)` 中断其余 Running，并完整保留 Queued。
- [x] RED：每个 SQL fault point 都证明 Task/failure/event/last_event_id 和 high watermark 全有或全无；commit-before-reply 重放不追加事件。
- [x] RED：`interrupt_remaining_after_stops`只负责generic interrupt；只要仍有任何Running intent就fail closed，绝不自行完成intent或假设进程树已退出。
- [x] RED：旧 `recover_incomplete` 的“Queued 也 Interrupted”生产路径被封死；Project 3 reviews/readiness 和历史 Completed 不漂移。
- [x] GREEN：实现cold-only `recover_after_restart`和guarded `interrupt_remaining_after_stops`及committed membership high watermark；final-stop仍只使用任务6事务并由任务18/20的process owner在cleanup proof后调用，Store层不伪造进程证明。
- [x] REFACTOR：投影查询同时提供 Scheduler 所需的 Queued、Running durable intents、latest event 与 membership watermark typed snapshot，不把动态 reason 写库。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test recovery --locked --offline -- --nocapture
cargo test -p coding-agent-store --test tasks --locked --offline
cargo test -p coding-agent-store --test reviews --locked --offline
cargo test -p coding-agent-store --test projection --locked --offline
```

检查点：冷启动只中断已开始的 Running；Queued 原 task.queued event 和排序不变。

## Checkpoint B：纯调度、仓库和资源 capability

## 任务 8：建立 authenticated common-Git 对象身份

**文件：**

- 修改 `crates/coding-agent-runtime/src/{command_policy,root_capability,worktree,lib}.rs`
- 修改 `crates/coding-agent-runtime/tests/{path_security,worktree}.rs`
- 修改 `crates/coding-agent-store/src/repositories.rs`
- 修改 `crates/coding-agent-store/tests/repositories.rs`

- [x] RED：从authenticated common Git directory capability取得marker；同一对象的case/SUBST/bind-mount/Cargo workspace aliases marker相等，同一路径替换为新目录后marker不同。
- [x] RED：Windows 使用平台 file identity，Unix 使用 device/inode 或等价 authenticated identity；无法可靠取得/比较时 fail closed，不退回 path string。
- [x] RED：每次 Git side effect 前后重验common directory capability和live marker；替换/漂移返回typed mismatch且不自行决定app poison或暴露路径。
- [x] GREEN：runtime只导出可比较、可散列但不可伪造/不可反解路径的`DirectoryIdentityMarker`；构造只能来自authenticated capability，不定义coordination/permit/poison状态。
- [x] GREEN：Store仅提供内部 `repository_id + git_identity_key` lookup projection，不修改公开 Repository/domain/API DTO。
- [x] REFACTOR：统一现有 Git root/common-dir/HEAD validation，保留 Project 2 Windows 大小写和 reparse 防护。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test path_security --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test worktree --locked --offline -- --nocapture
cargo test -p coding-agent-store --test repositories --locked --offline
```

检查点：公开 DTO、日志和 error message 不含 marker、common-dir 或绝对路径。

## 任务 9：实现纯 Scheduler 状态机、公平扫描与 permit ledger

**文件：**

- 新建 `crates/coding-agent-app/src/scheduler.rs`
- 新建 `crates/coding-agent-app/tests/scheduler.rs`
- 修改 `crates/coding-agent-app/src/lib.rs`

- [x] RED：按 `(created_at,task_id)` tie-break；同 coordination key 不超车；仓库 capacity/control/storage 阻塞时允许其他 key 的较新 Task 前进。
- [x] RED：五种唯一 reason 固定优先级 `service_paused > storage_pressure > global_capacity > repository_capacity > repository_control_busy`，React 不参与推导。
- [x] RED：global/repository limits `1..=4`、provisional reservation、adopt、known-not-applied release、unknown retain、terminal+process-clean release、double release 和 actor panic。
- [x] RED：generation 只随公开语义变化；raw sample数值、相同通知和中间被 coalesce 的状态不推进；membership/service 水位随 projection 保存。
- [x] RED：`as_of_event_id`只由`task.queued|task.started|task.completed|task.failed|task.cancelled|task.interrupted`六种membership lifecycle event推进；plan/activity/diff/test/review及诊断event绝不推进。
- [x] GREEN：在app定义由opaque marker分组的`RepositoryCoordinationKey`，实现纯候选扫描、opaque permit ownership token、reason projector 和只保留最新 immutable snapshot 的 watch publisher。
- [x] GREEN：Scheduler 只消费 typed Store snapshot、identity mapping、lease/storage/service gate 和 runtime config，不读取日志、event 文本或 UI。
- [x] REFACTOR：将纯决策与异步 claim side effect 分开，便于 exhaustive deterministic tests。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test scheduler --features test-support --locked --offline -- --nocapture
```

检查点：等待 repository control lease 的 Queued Task 不占 active permit。

## 任务 10：实现 RepositoryControlCoordinator 与 alias/poison 语义

**文件：**

- 新建 `crates/coding-agent-app/src/repository_control.rs`
- 新建 `crates/coding-agent-app/tests/repository_control.rs`
- 修改 `crates/coding-agent-app/src/{lib,repository_service}.rs`

- [x] RED：app把durable `git_identity_key` seed解析为live marker；多个repository_id/path key命中同marker时共享一个coordination key和control lease，不同marker可以独立推进。
- [x] RED：同一durable seed解析到新marker、marker不可观察或side-effect前后typed mismatch时poison该coordination key及全部aliases，不把新对象当旧仓库。
- [x] RED：Scheduler 只能 non-blocking `try_acquire`；busy 不排队持有 global/repository permit；lease 无超时强制转交。
- [x] RED：abnormal drop、Git child 未知、ready/inconsistent durable write 失败、identity drift 均 poison 全部 aliases；只有有证据校正后 clean release。
- [x] GREEN：实现durable-seed→marker→coordination-key mapping、keyed coordinator、alias registry、owned lease guard和显式`clean_release|poison` completion；guard drop默认poison。
- [x] REFACTOR：固定不允许 StoreWriter/runtime callback 在持有自身锁时反向获取 coordinator，增加 lock-order regression harness。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test repository_control --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test repository_service --features test-support --locked --offline
```

检查点：coordinator只拥有control lease/alias/poison；repository permit由任务9/17的Scheduler ledger按同一key拥有，且不创建自定义`.git`锁或声称阻止外部Git。

## 任务 11：把 worktree reservation/provisioning/reconciliation 放入 lease

**文件：**

- 新建 `crates/coding-agent-app/src/run_context.rs`
- 修改 `crates/coding-agent-app/src/{coding_agent_runner,runner_factory,artifact_reconciliation,lib}.rs`
- 修改 `crates/coding-agent-app/tests/{coding_agent_runner,artifact_reconciliation}.rs`
- 修改 `crates/coding-agent-runtime/src/worktree.rs`
- 修改 `crates/coding-agent-runtime/tests/worktree.rs`

- [x] RED：lease 覆盖 pre-validation、deterministic branch/path、durable reserved、Git side effect、post-validation、durable ready/inconsistent；SQLite transaction 不跨 Git。
- [x] RED：reservation前失败、确认无side effect、exact ready、partial/mismatch和I/O unavailable精确映射为无artifact、`inconsistent + WORKTREE_RESERVATION_ABANDONED`、ready、`inconsistent + WORKTREE_STATE_INCONSISTENT`、保留reserved。
- [x] RED：ready commit 后立即 clean release，角色循环不持 lease；ready/inconsistent outcome unknown 时 poison 并校正，不能启动 role 或提前释放。
- [x] RED：startup reserved reconciliation 同 identity 串行、不同 identity 有界并行，使用新 token；不删除 branch/worktree，不把 unavailable 猜成 inconsistent。
- [x] GREEN：把现有 runner 拆为 lease-owned attempt preparation 和 prepared worktree 上的角色执行；`RunContext` 保留 claim resources、artifact identity、process ownership 和一次性释放状态。
- [x] GREEN：artifact reconciliation 提供 startup direct-Store adapter 与生产 StoreWriter adapter，两者共享 observation/decision但边界不可混用。
- [x] REFACTOR：统一前后 capability validation，移除 runner 内部绕开 coordinator 的 reservation/provisioning 路径。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test worktree --locked --offline -- --nocapture
cargo test -p coding-agent-app --test repository_control --features test-support --locked --offline
cargo test -p coding-agent-app --test artifact_reconciliation --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test coding_agent_runner --features test-support --locked --offline -- --nocapture
```

检查点：P4-A 只保留现场和诊断，不执行 cleanup 或 repair。

## 任务 12：实现跨平台 volume identity 与 available-space sampler

**文件：**

- 新建 `crates/coding-agent-runtime/src/storage.rs`
- 新建 `crates/coding-agent-runtime/tests/storage.rs`
- 修改 `crates/coding-agent-runtime/src/lib.rs`
- 按需修改 workspace/runtime `Cargo.toml`，只使用 lockfile 可离线解析的依赖

- [x] RED：Windows stable volume identity、Unix mount/device identity、同卷 aliases 去重、不同卷独立；必跑逻辑用fake identities覆盖不同卷，真实不同卷只在平台fixture可用时条件验证，identity不可公开序列化。
- [x] RED：读取当前 OS 用户 available bytes，不用包含保留块的 total free；权限、卸载、不支持和 overflow 返回 typed unavailable。
- [x] GREEN：实现 capability-bound `VolumeSampler` port 和平台 adapters；结果仅含 opaque identity、current-user available bytes和 typed failure，由任务14负责in-flight与sample时间。
- [x] REFACTOR：真实卷测试仅采样隔离 temp roots，不填充或修改开发机磁盘。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test storage --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --all-targets --locked --offline
```

检查点：runtime 不知道 Scheduler、Task 或公开 storage DTO。

## 任务 13：实现纯 storage policy、准入、滞回和影响集合

**文件：**

- 新建 `crates/coding-agent-app/src/storage_policy.rs`
- 新建 `crates/coding-agent-app/tests/storage_policy.rs`
- 修改 `crates/coding-agent-app/src/lib.rs`

- [x] RED：data 公式、Git/runtime 256 MiB、相同物理卷取最严格谓词且不重复相加；等于阈值允许，全部计算 checked。
- [x] RED：critical严格为data `<512 MiB`、Git/runtime `<64 MiB`；等于critical阈值不进入critical。
- [x] RED：聚合优先级 `critical > unavailable > pressure > normal`；退出 pressure/critical/unavailable 需要间隔至少 5 秒的两次带 margin 成功样本，失败重置计数，单个正常样本不能恢复准入。
- [x] RED：critical/unavailable后的第一次带margin成功样本仍阻止admission，至少间隔5秒的第二次才恢复；data margin为next-candidate阈值`+512 MiB`，Git/runtime为`256 MiB + 64 MiB`。
- [x] RED：data/runtime critical 影响全部 active、Git critical 只影响同卷 identities、重合取并集且每 Task 一次；普通 pressure 不触发 cancel。
- [x] GREEN：实现无异步、无 Store、无系统时钟读取的 admission/classification/aggregate/hysteresis 纯状态机；时间戳、sample和 active mapping全部由调用者传入。
- [x] REFACTOR：用 exhaustive table覆盖 next-candidate `min(max,active+1)`、critical严格小于和恢复 margins。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test storage_policy --features test-support --locked --offline -- --nocapture
```

检查点：pure policy 不知道 TaskManager、StoreWriter、SSE或平台路径。

## 任务 14：实现 StorageMonitor actor 与 fresh-sample 通知

**文件：**

- 新建 `crates/coding-agent-app/src/storage_monitor.rs`
- 新建 `crates/coding-agent-app/tests/storage_monitor.rs`
- 修改 `crates/coding-agent-app/src/{scheduler,lib}.rs`

- [x] RED：sample fresh `<=5s`、有 queued/active 时5秒周期、空闲不轮询；stale admission先刷新，unavailable阻止新任务但不停止Running。
- [x] RED：data/runtime/repository Git logical scopes正确注册到opaque volume identity；共享卷只probe一次但同时应用所有scope谓词，repository alias不会重复采样或漏掉受影响Task。
- [x] RED：同一 volume最多一个 in-flight probe，多个逻辑 scope/调用者合并等待；2秒 timeout、权限、卸载转为 unavailable且不堆积 future。
- [x] RED：classification变化才通知 Scheduler；普通 raw byte变化不推进 generation，首次进入 critical立即走独立高优先级通知。
- [x] RED：raw bytes、volume identity和path不进入公开 snapshot或普通日志；monitor不持 StoreWriter、repository lease或permit。
- [x] GREEN：维护 logical-scope→volume、volume→sample/in-flight和scope hysteresis，调用任务13纯策略；以注入 clock/sampler驱动。
- [x] REFACTOR：分离 admission refresh request、周期采样和critical notification，使慢卷不饿死其他卷。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test storage_monitor --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test storage_policy --features test-support --locked --offline
cargo test -p coding-agent-app --test scheduler --features test-support --locked --offline
```

检查点：StorageMonitor不写Store、不直接决定Task终态。

## 任务 15：实现 process-liveness sentinel 与跨崩溃 cleanup proof

**文件：**

- 新建 `crates/coding-agent-runtime/src/process_liveness.rs`
- 新建 `crates/coding-agent-runtime/tests/process_liveness.rs`
- 修改 `crates/coding-agent-runtime/src/{process_supervisor,command_policy,tool_discovery,git_tools,worktree,cargo_tools,diff,fingerprint,runtime_session,lib}.rs`
- 修改 `crates/coding-agent-app/src/{single_instance,runner_factory,run_context,coding_agent_runner}.rs`
- 修改 `crates/coding-agent-app/tests/process_support.rs`

- [x] RED：Unix protocol 内子孙继承 file-description lock，Windows Job Object 继续 `KILL_ON_JOB_CLOSE` 并提供等价独占 probe；父 runner 返回不等于整树退出。
- [x] RED：取得primary lock后立即生成唯一instance UUID；每棵树以instance UUID、可选task UUID和独立tree nonce构造名称，同Task多棵树不冲突；held sentinel绝不删除，只有独占probe成功后才删除stale file。
- [x] RED：symlink/reparse替身、名称/内容伪造和格式错误sentinel均fail closed，不能借文件存在或mtime推断旧树已退出。
- [x] RED：cleanup timeout、process group/job race、grandchild 存活、primary 崩溃后新 probe 和最终释放；平台无法实现时 fail closed。
- [x] GREEN：所有生产`ProcessSupervisor`构造都强制接收`ProcessLivenessScope`，无scope构造仅允许`cfg(test)`；startup tool discovery使用instance scope，Git/worktree/Cargo/diff/fingerprint等Task子进程使用task scope，当前HTTP provider不伪装成子进程，未来若启动本地进程也必须接scope。
- [x] GREEN：“确认退出”同时要求scope registry为空、OS tree proof和exclusive sentinel probe。
- [x] REFACTOR：将本地启动诊断与TaskFailure分开；`PROCESS_TREE_CLEANUP_FAILED`只在无stop winner且最终取得cleanup proof后才可成为TaskFailure，已有intent时永不覆盖winner。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test process_liveness --locked --offline -- --nocapture
cargo test -p coding-agent-runtime process_supervisor --lib --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test cargo_tools --locked --offline
cargo test -p coding-agent-app --test process_support --features test-support --locked --offline -- --nocapture
```

检查点：测试结束必须确认辅助子进程均退出，不遗留 held sentinel。

## Checkpoint C：生产 TaskManager、StoreWriter 与启动/关闭

## 任务 16：扩展 StoreWriter priority、sequence 与 typed dispositions

**文件：**

- 新建 `crates/coding-agent-app/src/pending_durable.rs`
- 修改 `crates/coding-agent-app/src/store_writer.rs`
- 修改 `crates/coding-agent-app/src/{shutdown,single_instance,lib,test_support}.rs`
- 修改 `crates/coding-agent-app/tests/store_writer.rs`
- 修改 `crates/coding-agent-app/tests/support/mod.rs`

- [x] RED：新增 queue-limited create/retry、claim、stop intent batch、final-stop 和受影响质量操作；所有 receipt 保留 `Applied|ExistingApplied/Existing|KnownNotApplied|OutcomeUnknown|InvariantConflict` 等精确信息。
- [x] RED：urgent batch 可越过尚未开始的其他 Task 普通写，但不能越过当前 in-flight transaction、同 Task 更早 sequence或已开始 finalization；最多四项共享一个 transaction。
- [x] RED：channel closed、deadline、commit-before-wake、commit-before-reply、known rollback/busy 和 connection loss 映射不同 disposition；unknown 不释放资源或伪造 ack。
- [x] RED：同 Task sequence有空洞或反转时fail closed；urgent持续到达时其他Task不被无界饿死，completion始终携带原operation identity和certainty。
- [x] GREEN：实现 bounded normal/urgent ingress、per-task sequence barrier、query-first replay和 typed completion；writer仍是唯一生产 mutation owner。
- [x] GREEN：切换 create/retry 生产入口到 queue-limited Store API，移除 production unlimited bypass。
- [x] REFACTOR：`PendingDurableResult` 只保存能精确幂等重放的 typed request；普通 panel append 不获得虚假幂等键。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test store_writer --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-store --test queue_capacity --locked --offline
cargo test -p coding-agent-store --test claims --locked --offline
cargo test -p coding-agent-store --test stop_intents --locked --offline
cargo check -p coding-agent-app --all-targets --features test-support --locked --offline
```

检查点：urgent 是安全优先级，不是同 Task 线性化顺序的旁路。

## 任务 17：接入 Scheduler claim、RunContext adoption 与并发 runner

**文件：**

- 修改 `crates/coding-agent-app/src/{task_manager,scheduler,run_context,coding_agent_runner,runner_factory,store_writer}.rs`
- 修改 `crates/coding-agent-app/tests/{task_manager,coding_agent_runner}.rs`

- [x] RED：global=2/repository=2 时两个同仓库 Task 在 worktree ready 后角色循环真实重叠，第三个及其他仓库 Task 均为更高优先级的 global_capacity；另以 global>=3/repository=2 证明第三个同仓库为 repository_capacity，其他仓库可跳过启动。
- [x] RED：普通StoreWriter写入lag或等待completion时TaskManager mailbox仍立即处理safety latch、高优先级notification和shutdown；handler只提交写，completion作为新message返回。
- [x] RED：claim 覆盖 Applied、ExistingApplied、KnownNotApplied、OutcomeUnknown、InvariantConflict；unknown 保留 provisional permits/lease且不 spawn，精确查询后只可 release 或 adopt。
- [x] RED：集成event trace固定为fresh service/storage -> reserve global/repository permits -> non-blocking control lease -> claim commit -> adopt RunContext -> publish ActiveSafetyRegistry -> final gate recheck -> spawn；lease busy立即释放provisional permits且不得调用claim，SQLite transaction不跨Git side effect。
- [x] RED：统一 `adopt_and_maybe_launch` 在资源仍受拥有时发布 `RunContext` 和最多四项的 `ActiveSafetyRegistry`，再检查 service/gate/shutdown/critical/latch/cancel/stop/storage；关闭后绝不启动新副作用。
- [x] RED：runner success/failure/panic、claim callback重复、actor close 和 task already terminal 不超发、不 double release；permit 只在终态 commit且 process tree退出后释放。
- [x] RED：ActiveSafetyRegistry发布时立即重查当前critical scopes，关闭采样通知与runner spawn之间的漏停窗口；terminal event水位进入Scheduler projection后才释放permit并重扫。
- [x] RED：全部完成路径固定为cancel/stop并等待所有trees -> cleanup proof -> terminal transaction commit -> Scheduler记录terminal watermark -> release permits/ownership -> rescan；任何提前terminal或提前release都由测试拒绝。
- [x] GREEN：以 Scheduler scan替换固定单并发启动路径；TaskManager 分配 per-task mutation sequence，异步提交 claim并以 completion mailbox 接管。
- [x] GREEN：preparation worker持有 repository lease到 artifact ready/inconsistent commit，随后角色 worker只持 global/repository permits和 process ownership。
- [x] REFACTOR：集中 release state machine，所有正常、panic、cancel、shutdown、degraded 路径复用一次性释放证明。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test task_manager --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test coding_agent_runner --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test repository_control --features test-support --locked --offline
```

检查点：active 线性化点是 `task.started` commit，不是 permit acquire 或 future spawn。

## 任务 18：接入用户取消、critical safety 与 quality/finalization 竞态

**文件：**

- 修改 `crates/coding-agent-app/src/{task_manager,storage_monitor,store_writer,pending_durable,run_context,coding_agent_runner,shutdown}.rs`
- 修改 `crates/coding-agent-app/tests/{task_manager,store_writer,degraded_recovery,shutdown}.rs`

- [x] RED：Queued cancel继续在既有单事务中直接Cancelled且不建intent；Running user cancel先由actor固定winner/sequence，intent commit后才返回accepted并触发token；同intent replay accepted、terminal返回现有Task、disk-first返回typed conflict。
- [x] RED：critical observation 先通过 ActiveSafetyRegistry 原子 latch/kill，不等待 actor/Store；TaskManager 合并任务并给整批共享 1 秒 persistence budget。
- [x] RED：user-first、disk-first、finalization-first、stop-sequence-first、commit-before-reply和 late Approved/Rejected/Failed/Cancelled 全排列只产生一个规定终态。
- [x] RED：stop 后在途 activity/diff/test 可有界提交但不能写 readiness或越过 sequence；process tree未退出时不得 final-stop。
- [x] RED：所有生产停止顺序都是cleanup proof ->任务6 final-stop ->任务7 generic interrupt；任一步未知时不释放permit、不越过winner且不调用后续Store helper。
- [x] RED：urgent persistence 超预算进入 frozen/degraded，只把 durable intents投影为 stopping；本地 latch/pending write 不冒充 durable事实。
- [x] GREEN：实现 first-accepted winner、out-of-band cancellation、urgent batch completion和 intent-aware runner outcome barrier；进程退出后调用 exact final-stop。
- [x] REFACTOR：统一用户/磁盘停止的 process cleanup，只有 terminal mapping和对外 response 不同。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test task_manager --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test store_writer --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test degraded_recovery --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test shutdown --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-store --test reviews --locked --offline
cargo test -p coding-agent-store --test stop_intents --locked --offline
```

检查点：disk critical kill 可以早于 durable commit，但 UI 只能看到已经 commit 的 stopping intent。

## 任务 19：实现内部 Bootstrap exact join 并复用 primary instance identity

**文件：**

- 新建 `crates/coding-agent-app/src/bootstrap_join.rs`
- 修改 `crates/coding-agent-app/src/{server,single_instance,scheduler,lib,test_support}.rs`
- 修改 `crates/coding-agent-app/tests/server.rs`
- 修改 `crates/coding-agent-store/src/{projection,lib}.rs`
- 修改 `crates/coding-agent-store/tests/projection.rs`

- [x] RED：single-instance descriptor、runtime router和Scheduler epoch复用任务15在取得primary lock后生成的同一个UUID v4；不能在backend构造后各自生成。
- [x] RED：bounded join按 `S1 -> consistent Store snapshot M -> Q -> S2`；只有 `S1==S2==Q.service_state_generation`、`Q.as_of_event_id==M`和Task/intent/repository集合精确匹配才成功。
- [x] RED：Q落后M时等待scheduler watch追平；Q超前、service变化或集合漂移时重读Store；固定预算耗尽返回typed `BOOTSTRAP_SNAPSHOT_UNAVAILABLE`。
- [x] RED：Store snapshot精确包含repositories、tasks、Running durable intents、latest event和membership watermark；终态intent只校验immutable audit，不投影为stopping。
- [x] GREEN：Store用单个一致read transaction返回内部`SchedulerBootstrapSnapshot`，包含repositories、tasks、Running intents、latest event和membership watermark；app抽出不依赖API DTO的bounded retry join，禁止多次Store调用或API全表replay拼快照。
- [x] REFACTOR：使用注入clock/budget/watch和deterministic mutation hooks证明每条重试分支，不返回近似拼接结果。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test server --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --all-targets --features test-support --locked --offline bootstrap_join
cargo test -p coding-agent-store --test projection --locked --offline -- --nocapture
```

检查点：本任务只建立内部一致logical snapshot，不改变公开Bootstrap wire shape。

## 任务 20：重排 startup、graceful shutdown 与 in-process degraded recovery

**文件：**

- 修改 `crates/coding-agent-app/src/{single_instance,provider_config,runtime_config,runner_factory,repository_control,storage_monitor,artifact_reconciliation,event_dispatcher,bootstrap_join,task_manager,shutdown,server,service_state,test_support}.rs`
- 修改 `crates/coding-agent-app/tests/{single_instance,artifact_reconciliation,event_dispatcher,shutdown,degraded_recovery,server}.rs`

- [x] RED：启动严格为 lock -> runtime/provider config与private paths -> sentinel probes -> SQLite history/migrate -> coordinator -> startup direct artifact reconciliation -> atomic recovery -> dispatcher at high watermark -> StoreWriter/TaskManager/Scheduler -> fresh storage/exact Bootstrap -> Web Ready。
- [x] RED：held sentinel 时不得打开/迁移 SQLite、recover Running或监听 Web，并继续持有primary lock在本地重试cleanup proof；reserved artifact暂时不可观察时不开放Ready也不借用StoreDegraded。
- [x] RED：invalid provider/runtime config或held/unknown sentinel时StoreFactory `open/migrate`和listener `bind`调用数均为0；sentinel phase不能返回会正常释放primary lock的普通startup error，proof释放后自动继续同一startup。
- [x] RED：recovery commit与receipt之间崩溃、dispatcher wake丢失、startup high watermark、Queued重扫和 durable Running intents重建不漏发/重复 event。
- [x] RED：graceful/degraded 先 pause/gate、解析 typed pending和 process trees，再完成 durable intents，最后 interrupt其余；flush后才 release/Ready。
- [x] RED：shutdown/degraded必须对全部active取得cleanup proof，逐Task调用任务6 final-stop确认intent已终态化，再调用任务7 guarded generic interrupt；Store helper本身不能替代process proof。
- [x] RED：退出预算耗尽且树未知时 listener关闭但 primary保留 single-instance lock、handles、permits和 ownership持续 cleanup；只有全部树证明退出才允许有限退出。
- [x] RED：Store不可写但全部process trees已证明退出时可写既有degraded-shutdown marker并有限非零退出；任一tree未知时该有限退出例外不可用，且无stop winner时cleanup failure只有最终清理后才可成为TaskFailure。
- [x] GREEN：把RunnerFactory拆为pre-DB不可变`ValidatedStartupInputs`和post-recovery runner construction；后者禁止重读配置或执行startup reconciliation。重排single-instance composition并删除旧“启动即中断Queued”和“10秒后无条件释放lock”的路径，用任务19 exact join作为Ready硬门。
- [x] GREEN：startup direct artifact reconciliation在Dispatcher、StoreWriter、TaskManager和listener构造前完整结束；任务15生成的instance identity贯穿preflight、sentinel、descriptor和Scheduler。
- [x] REFACTOR：用显式 startup phase types防止 StoreWriter/direct Store或 listener过早构造；所有测试夹具显式提供 RuntimeConfig。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test single_instance --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test provider_config --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test runtime_config --features test-support --locked --offline
cargo test -p coding-agent-app --test artifact_reconciliation --features test-support --locked --offline
cargo test -p coding-agent-app --test event_dispatcher --features test-support --locked --offline
cargo test -p coding-agent-app --test shutdown --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test degraded_recovery --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test server --features test-support --locked --offline
cargo test -p coding-agent-runtime --test process_liveness --locked --offline -- --nocapture
```

检查点：startup direct Store exception在 production actors和 listener出现前结束。

## Checkpoint D：原子 wire contract 与 React 投影

## 任务 21：原子增加 Scheduler Bootstrap、queue-full HTTP 与 Web snapshot state

**文件：**

- 修改 `crates/coding-agent-api/src/{contract,backend,router,error}.rs`
- 修改 `crates/coding-agent-api/tests/{openapi,router}.rs`
- 修改 `crates/coding-agent-app/src/{server,task_manager,scheduler}.rs`
- 修改 `crates/coding-agent-app/tests/server.rs`
- 修改 `web/openapi.json`
- 修改 `web/src/api/{types,validation,client}.ts` 及测试
- 修改 `web/src/api/generated/schema.d.ts`
- 修改 `web/src/state/{model,reducer,useAgentState}.ts` 及测试

- [x] RED：Bootstrap required exact `scheduler`、全部 bounds/enums/sorting/cross constraints、完整 legacy queue、Running durable intents和每 repository storage；UUID必须canonical lowercase且`server_instance_id`为v4，顶层 max alias、started_at、service generation和 membership watermark必须精确相等。
- [x] RED：startup捕获有限`legacy_queued_count`后queued数组只能下降或在低于配置limit时再增长；任何公开计数无法表示为u32时以`DATABASE_PROJECTION_LIMIT_EXCEEDED` fail closed而非截断/回绕。
- [x] RED：任务20的内部join失败映射为 exact `503 BOOTSTRAP_SNAPSHOT_UNAVAILABLE`；成功结果逐字段投影且不再次近似读取Store/Service/Scheduler。
- [x] RED：真正新create/retry满队列返回`429 TASK_QUEUE_FULL` exact envelope和request_id且无Retry-After；Existing/idempotency conflict/not-retryable优先级保持。另一stop intent已获胜的Running cancel映射`409 TASK_STOP_ALREADY_REQUESTED`，不改变winner。
- [x] RED：TypeScript exact validator拒绝未知/缺失/null/unsafe integer/非 canonical UUID/排序或交叉约束错误；Bootstrap reducer原子采用 snapshot，不从 Task本地推导 reason/storage/stop。
- [x] GREEN：在同一变更中完成 Rust DTO/backend/router/OpenAPI export/generated TS/client validator/state fixtures；复用 single-instance UUID作为 scheduler epoch。
- [x] GREEN：前端保存 scheduler snapshot、fresh/stale元数据、membership/service watermarks和 queue-full replay所需原 prompt/client_request_id；Bootstrap采用时把`appliedMembershipEventId`初始化为`scheduler.as_of_event_id`，但尚不增加最终视觉布局。
- [x] REFACTOR：共享 logical storage聚合和 exact-object验证器，公开 DTO不含 raw bytes、volume ID、路径或 config path。
- [x] 验证：

```powershell
cargo test -p coding-agent-api --test openapi --locked --offline
cargo test -p coding-agent-api --test router --locked --offline -- --nocapture
cargo test -p coding-agent-app --test server --features test-support --locked --offline -- --nocapture
cargo run --locked --offline -p coding-agent-api --bin export_openapi -- web/openapi.json
npm --prefix web run api:generate
npm --prefix web run api:check
npm --prefix web run test:run -- src/api/validation.test.ts src/api/client.test.ts src/state/reducer.test.ts src/state/useAgentState.test.tsx
npm --prefix web run typecheck
```

检查点：TaskDto、TaskDetailDto、CancellationAcceptedResponse和持久 TaskEvent payload不增加 scheduler字段。

## 任务 22：原子增加 SSE manifest/chunk、RFC 8785 digest 与客户端精确因果门

**文件：**

- 新建 `crates/coding-agent-api/src/scheduler_wire.rs`
- 修改 `crates/coding-agent-api/{Cargo.toml,src/contract.rs,src/lib.rs}`
- 修改 `crates/coding-agent-api/tests/{openapi,sse}.rs`
- 修改 `crates/coding-agent-app/src/{server,scheduler}.rs`
- 修改 `crates/coding-agent-app/tests/server.rs`
- 修改 `web/openapi.json`
- 新建 `web/src/api/{schedulerSnapshot.ts,schedulerSnapshot.test.ts}`
- 修改 `web/src/api/{types,validation,sse}.ts` 及测试
- 修改 `web/src/api/generated/schema.d.ts`
- 修改 `web/src/state/{model,reducer,useAgentState}.ts` 及测试
- 新建 `testdata/scheduler-state-rfc8785.json`

- [x] RED：manifest/chunk均无 SSE id、不写 Store/不推进 event cursor；items固定 queued/stopping/repository-storage顺序，每 chunk最多128且每 frame序列化后 `<=64 KiB`。
- [x] RED：空/legacy超额、多 chunk、missing/duplicate/out-of-order/count/sort/digest/epoch冲突、新 generation抢占和同 generation不同 payload全部 fail closed或 bootstrap recovery。
- [x] RED：Rust/TypeScript对同一 full logical DTO生成逐字节相同 RFC 8785 canonical bytes和 lowercase SHA-256；覆盖 Unicode string、安全整数、固定 ASCII keys和边界数组。
- [x] RED：客户端只有 `applied_membership_event_id == as_of_event_id` 且 service generation精确相等才原子应用完整组装；客户端超前时丢弃旧 control而不是使用 `>=`。
- [x] RED：`applied_task_event_id`与`applied_membership_event_id`分离；只有六种membership lifecycle frame实际应用时推进后者，plan/activity/diff/test/review和未知诊断不推进。
- [x] RED：Bootstrap请求期间到达更高完整generation时，旧Bootstrap不得覆盖；同instance可比较tuple按generation/digest仲裁，不同instance或membership/service tuple不可比较时清空partial/current并重新Bootstrap+subscribe，`stream.reset`也清空partial。
- [x] GREEN：同一变更更新 `SseMessage.oneOf`、OpenAPI/generated TS、Rust emitter、Web parser/assembler/validator/reducer和fixtures；parser在持久 event ID之前识别两种无 ID control。
- [x] GREEN：客户端assembler同时只保留一个partial generation；新generation抢占旧partial，partial永不进入reducer。
- [x] REFACTOR：把 canonicalization限制在 exact Scheduler DTO值域并共享跨语言固定 vectors，不引入浮点或任意 JSON输入。
- [x] 验证：

```powershell
cargo test -p coding-agent-api --test openapi --locked --offline
cargo test -p coding-agent-api --locked --offline scheduler_wire
cargo test -p coding-agent-api --test sse --locked --offline -- --nocapture
cargo test -p coding-agent-app --test server --features test-support --locked --offline
cargo run --locked --offline -p coding-agent-api --bin export_openapi -- web/openapi.json
npm --prefix web run api:generate
npm --prefix web run api:check
npm --prefix web run test:run -- src/api/schedulerSnapshot.test.ts src/api/sse.test.ts src/api/validation.test.ts src/state/reducer.test.ts src/state/useAgentState.test.tsx
npm --prefix web run typecheck
```

检查点：持久 event仍恰好11种；scheduler control不参与 lifecycle recovery loop。

## 任务 23：实现服务端 SSE 因果发送、公平性与有界 coalescing

**文件：**

- 修改 `crates/coding-agent-api/src/{backend,sse}.rs`
- 修改 `crates/coding-agent-api/tests/{sse,support/mod}.rs`
- 修改 `crates/coding-agent-app/src/server.rs`
- 修改 `crates/coding-agent-app/tests/server.rs`

- [x] RED：task live、service state和scheduler watch三路订阅都发生在第一次可能等待的Store fetch之前，连接后立即发送当前service.state。
- [x] RED：慢latest-event fetch、慢replay、大legacy分段、live lag和heartbeat期间service control与15秒heartbeat不饥饿。
- [x] RED：初始/live scheduler都等待membership/service精确门；backend使用明确的`membership_watermark_through(after_cursor)`，且只有六种lifecycle frame实际yield后推进；`task.started`/terminal event必须先于对应active/stopping变化，future水位缓存、stale水位丢弃。
- [x] RED：service generation在分段期间变化、stream.reset或新scheduler generation可中止旧分段；scheduler control无id且不推进persisted cursor。
- [x] RED：cursor停在非membership event后且数据库尚有待replay membership event、reset/reconnect与chunk交错、Bootstrap延迟期间收到更高generation时都保持exact gate并按任务22仲裁。
- [x] RED：慢连接同时最多一个in-flight generation，publisher只保留最新`Arc`/等价immutable snapshot；中间generation coalesce，内存不随历史或客户端速度增长。
- [x] GREEN：扩展`SseBackend`的snapshot/watch和membership watermark port，把scheduler候选纳入现有公平select loop并在chunk之间让出控制。
- [x] GREEN：序列化失败、超64KiB或无法完成一致分段时fail closed并触发现有reset/Bootstrap recovery。
- [x] REFACTOR：所有backend fakes实现同一typed port，避免测试通过绕过真实因果门的快捷接口。
- [x] 验证：

```powershell
cargo test -p coding-agent-api --test sse --locked --offline -- --nocapture
cargo test -p coding-agent-app --test server --features test-support --locked --offline -- --nocapture
cargo clippy -p coding-agent-api -p coding-agent-app --all-targets --all-features --locked --offline -- -D warnings
```

检查点：service.state wire shape不变，scheduler控制不能饿死既有安全状态。

## 任务 24：实现 capacity、queue reason、stale、stopping 与 queue-full UI

**文件：**

- 修改 `web/src/components/{AppShell,Sidebar,TaskComposer,TaskWorkspace}.tsx`
- 新建 `web/src/components/{SchedulerSummary.tsx,SchedulerSummary.test.tsx}`
- 修改相邻 component tests
- 修改 `web/src/styles.css`
- 修改 `web/src/state/useAgentState.test.tsx`

- [x] RED：显示 active/global、per-repository limit、queue usage和 cargo jobs；Queued只显示服务端五种固定文案，不显示位置或 ETA。
- [x] RED：SSE断开保留最后 snapshot但显式 stale且不当作准入事实；同 epoch/generation/digest完整重放可清 stale，新 epoch强制 Bootstrap。
- [x] RED：durable stopping禁用重复 cancel并区分 user/disk；最终分别显示 Cancelled和 retryable Failed，不能混用 readiness或 ReviewRejected。
- [x] RED：fresh snapshot队列满时禁用全新提交；结果未知的原 client_request_id显式重放仍可用，429保留 prompt/request id，容量恢复后允许重放。
- [x] RED：legacy `queued_count > limit`如实显示；UI不从 Task列表推断 reason/storage/winner，不出现 merge/cleanup/settings/artifact/history控件。
- [x] GREEN：实现最小可访问布局、固定文案、disabled/replay状态和长标识换行；复用 reducer中已验证的 scheduler snapshot。
- [x] REFACTOR：将纯显示映射集中测试，避免组件复制 enum优先级或 storage聚合。
- [x] 验证：

```powershell
npm --prefix web run test:run -- src/components/AppShell.test.tsx src/components/TaskWorkspace.test.tsx src/components/SchedulerSummary.test.tsx src/state/useAgentState.test.tsx
npm --prefix web run typecheck
npm --prefix web run build
```

检查点：UI使用“受控并发/准入”含义，不宣称 OS sandbox或磁盘硬配额。

## Checkpoint E：离线系统证明与最终验收

## 任务 25：补齐 concurrent E2E、崩溃/压力矩阵与文档

**文件：**

- 新建 `crates/coding-agent-app/tests/concurrent_offline_e2e.rs`
- 修改 `crates/coding-agent-app/tests/{offline_e2e,multi_role_offline_e2e}.rs`
- 修改 `crates/coding-agent-app/src/test_support.rs`
- 修改 `crates/coding-agent-app/tests/support/mod.rs`
- 新建 `web/e2e/concurrent-scheduler.spec.ts`
- 修改 `web/e2e/{support/localApp,fault-recovery,lifecycle,ui-edge-cases}.spec.ts`
- 修改 `README.md`

- [x] RED：临时真实 Git/Cargo仓库+scripted provider证明同仓库两个独立 branch/worktree在 ready后角色循环实际重叠，control phase严格串行且原工作目录dirty内容不进入 Task。
- [x] RED：第三个同仓库阻塞、第二仓库跨过；queue满/排空、legacy超额分段、storage pressure/critical/hysteresis、user/disk/finalization竞态。
- [x] RED：独立held-sentinel helper明确持有probe，新实例必须持primary lock等待且DB/recovery/admission调用数为0，释放后自动继续。
- [x] RED：实际primary强杀同时包含Running、Queued和durable intent；允许旧树仍held而等待，或Windows Job已kill-on-close且独占probe立即成功，两种都必须证明unknown cleanup proof从未被DB/recovery/admission越过并得到规定恢复结果。
- [x] RED：Unix另测继承lock的孙进程，Windows另测Job active count/kill-on-close；不能要求每次Windows强杀都可观察到held瞬间。
- [x] RED：浏览器Bootstrap响应被延迟时先收到更高完整scheduler generation；旧Bootstrap不能倒灌，epoch/tuple不可比较时可观察到重新Bootstrap和订阅。
- [x] RED：长轮次 create/start/finish、StoreWriter lag、输出洪泛、SSE慢客户端/重连不泄漏 permit、重复 terminal/event或按历史 generation无界增长。
- [x] GREEN：扩展 deny-unknown且全字段必填的 `ProcessTestConfig`、Rust/localApp/scenario literals；storage/process fakes可确定控制，不使用真实 provider或填满真实卷。
- [x] GREEN：README记录 runtime.json默认/范围、受控并发、queue reason、Cargo jobs、managed-volume边界、恢复、停止分类、无自动 merge/cleanup和非 OS sandbox。
- [x] REFACTOR：把并发 barrier、fake clock/sampler和process cleanup assertions放入共享 test support，测试结束验证无子进程/held sentinel。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test concurrent_offline_e2e --all-features --locked --offline -- --nocapture
cargo test -p coding-agent-app --test multi_role_offline_e2e --all-features --locked --offline -- --nocapture
cargo build --locked --offline -p coding-agent-app --features e2e
$env:CODING_AGENT_E2E_BINARY = (Resolve-Path '.\target\debug\coding-agent-app.exe').Path
try {
  npm --prefix web run e2e -- concurrent-scheduler.spec.ts fault-recovery.spec.ts lifecycle.spec.ts ui-edge-cases.spec.ts
} finally {
  Remove-Item Env:CODING_AGENT_E2E_BINARY -ErrorAction SilentlyContinue
}
```

检查点：网络 guard证明零真实 provider流量；P4-B/P4-C/P4-D能力仍不存在。

## 任务 26：独立代码审查与完整验收

- [x] 并行进行至少三路独立审查：
  - Store/迁移/事务：queue cap、intent immutable、final-stop、quality interlock、recovery原子性。
  - Scheduler/API/Web：fairness、permit、Bootstrap exact join、SSE chunk/digest/causal gate、stale projection。
  - Runtime/failure safety：Git identity/lease、storage、process sentinel、startup/shutdown/degraded和边界脱敏。
- [x] 解决全部 Blocker/High findings；每次修复先运行受影响聚焦测试，再使用最终代码运行完整门禁。
- [x] 人工核对最终 diff：没有 TaskStatus/event扩展，没有生产 Store bypass，没有 live provider尝试，没有 cleanup/merge/history/artifact lifecycle/dynamic config/packaging越界。
- [x] 执行：

```powershell
npm --prefix web ci
$openApiCheck = New-TemporaryFile
cargo run --locked --offline -p coding-agent-api --bin export_openapi -- $openApiCheck.FullName
if ((Get-FileHash -Algorithm SHA256 $openApiCheck.FullName).Hash -ne (Get-FileHash -Algorithm SHA256 'web\openapi.json').Hash) { throw 'web/openapi.json is out of date' }
Remove-Item -LiteralPath $openApiCheck.FullName
npm --prefix web run api:check
npm --prefix web run config:check
npm --prefix web run typecheck
npm --prefix web run test:run
npm --prefix web run build
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo test --workspace --all-targets --all-features --locked --offline
cargo build --locked --offline -p coding-agent-app --features e2e
$env:CODING_AGENT_E2E_BINARY = (Resolve-Path '.\target\debug\coding-agent-app.exe').Path
try {
  npm --prefix web run e2e
} finally {
  Remove-Item Env:CODING_AGENT_E2E_BINARY -ErrorAction SilentlyContinue
}
cargo build --release --locked --offline -p coding-agent-app --features embedded-web
node scripts/check-placeholders.mjs
git diff --check
```

macOS/Linux 的 E2E 环境变量命令替换为：

```bash
CODING_AGENT_E2E_BINARY="$PWD/target/debug/coding-agent-app" npm --prefix web run e2e
```

## 完成定义

只有以下条件全部满足，P4-A 才可报告完成：

- 任务 1–25 均有先失败、后通过的聚焦测试证据，且最终工作区通过任务 26 完整门禁。
- 独立审查 Blocker/High 为零，最终 diff 与已批准规格逐条核对。
- 默认 global/per-repository 并发为 2，最大 4；queue默认32且事务内不超卖。
- permit、lease、process ownership、stop winner和quality finalization在所有成功/失败/未知结果路径下线性化且无泄漏。
- v1/v2/v3→v4、cold/graceful/degraded恢复、Bootstrap/SSE/React因果投影均有离线确定性证明。
- 六态 Task、11种持久 event、Project 3 readiness/evidence和不自动 merge/cleanup的边界保持。
- 没有执行或暗示任何真实 provider尝试，没有开始 P4-B、P4-C或P4-D。
