# Project 4A：受控并发与资源准入设计

> 日期：2026-07-27
> 状态：已于 2026-08-03 完成实现、独立审查与完整验收
> 前置条件：Project 3 已完成并验收
> Project 4 范围：仅 P4-A；合并、清理、历史分页、制品生命周期和发行属于后续里程碑

## 1. 目标

P4-A 把 Project 2/3 的生产真实任务全局并发从固定 `1` 提升为默认 `2`，同时保持一任务一分支、一 worktree、一 provider task session 和一套质量证据边界。并发不是简单扩大 semaphore：任务只有在全局容量、同 Git 仓库容量、仓库控制 lease 和三类磁盘准入全部满足后，才能从 `Queued` 原子进入 `Running`。

本阶段还为并发运行补齐确定性的排队原因、FIFO 公平性、Cargo 并行度、队列背压、磁盘压力、持久停止意图、崩溃恢复以及 Bootstrap/SSE/React 投影。目标是让多个真实任务能够安全地重叠执行，而不是提供 OS 级 CPU、内存、进程数、网络或文件系统沙箱。

`TaskStatus` 仍只描述六态生命周期；排队原因、scheduler pause、storage pressure、控制锁和“正在停止”都是独立控制面投影。Project 3 的 `delivery_readiness`、generation/digest 证据和 Reviewer 质量事务继续保持原语义。

## 2. 范围

### 2.1 包含

- 生产真实任务的全局 active 上限和同 Git 仓库 active 上限。
- 默认全局并发 `2`、同仓库并发 `2`，最大允许配置为 `4`。
- 基于 `(created_at, task_id)` 的确定性调度、跨仓库跳过和同仓库不超车。
- 由现有规范化 `git_identity_key` 解析到 common Git directory 对象身份的应用内 `RepositoryControlCoordinator`。
- worktree reservation、Git side effect、观察和 durable `ready|inconsistent` 之间的控制 lease。
- 应用数据卷、仓库 Git 卷和 runtime 卷的空间采样、准入阈值、滞回和 critical stop。
- 按配置并发上限派生的稳定 `cargo_jobs_per_task`。
- 全局 `Queued` 硬上限、创建/retry 背压和幂等优先语义。
- 不可变 `task_stop_intents`、用户取消与磁盘安全停止的首次获胜规则。
- 冷启动、优雅关闭和进程内 Store degraded 三种恢复路径。
- typed scheduler Bootstrap 投影、非持久 `scheduler.state` manifest/chunk SSE control 和 React 展示。
- 离线、确定性的并发、竞态、迁移、磁盘、SSE、UI、压力和跨平台测试。

### 2.2 不包含

- merge、rebase、cherry-pick、冲突解决、push、PR 创建或自动交付；这些属于 P4-B。
- worktree/branch 的用户保留、删除、自动清理或回收策略；这些属于 P4-B。
- 历史分页、搜索、制品实际大小核算、保留期和删除配额；这些属于 P4-C。
- provider/命令全链路的新脱敏体系、安装器、签名、公证、平台包装或真实 provider 冒烟；这些属于 P4-D。
- 动态配置 UI、运行时热重载、按仓库排队上限或按 provider 单独限流。
- 恢复 provider transcript、继续被强杀的角色循环或复用旧 attempt 的可写 worktree。
- OS 级 CPU、内存、线程、进程数、网络、文件系统或宿主总磁盘硬配额。
- 约束用户在应用之外运行的 Git/Cargo/编辑器进程。

### 2.3 信任与安全边界

Project 2/3 的信任模型保持不变。Agent 第一方工具仍被 worktree capability、路径和命令策略约束，但 Cargo、`build.rs`、proc-macro、测试二进制以及仓库生成的子进程继续以当前 OS 用户权限运行。`--jobs=N` 只约束 Cargo 自身的作业并行度，不限制测试线程、进程数或代码主动创建的工作负载。

磁盘策略只保护应用明确管理和能够识别的路径：应用数据目录、登记仓库的 common Git directory 所在卷以及 runtime/temp 目录所在卷。Cargo 可能访问 `CARGO_HOME`、`RUSTUP_HOME`，受信仓库代码也可能写入任意宿主可写位置；P4-A 不把三卷监控描述为宿主磁盘沙箱。若未来要把这些路径纳入硬配额，必须另行设计重定向或 OS 隔离。

`RepositoryControlCoordinator` 只协调当前 primary 应用实例中的受控 Git 操作。它不创建自定义 `.git` 锁文件，也不能阻止外部 Git 命令。每次受控 Git side effect 前后仍必须验证 common Git directory、branch、HEAD、worktree admin metadata 和 retained capability 身份；发现外部变化时 fail closed。

## 3. 对 Project 1–3 的显式修订

路线图要求后续项目不得静默改变已验收行为。P4-A 明确修订以下旧边界：

1. Project 2/3 的生产真实任务全局并发固定 `1`，改为读取 P4-A 启动配置，默认 `2`、最大 `4`。
2. Project 1/3 冷启动把全部遗留 `Queued/Running` 改为 `Interrupted`；P4-A 改为只中断已经开始执行的 `Running`，从未开始的 `Queued` 保持排队并重新扫描。
3. Project 1 的优雅关闭把全部未完成任务统一标为 `Interrupted`；P4-A 先完成已经持久接受的用户取消或磁盘停止意图，再把其余未完成任务标为 `Interrupted`。
4. Project 3 认为冷启动不存在可恢复的取消事实；P4-A 仅为两个 typed stop intent 增加持久事实。未提交的 review/finalization 仍不得从 worktree 猜测。
5. Project 1 的 degraded shutdown 在 10 秒总预算后释放单实例锁并退出；P4-A 只在全部受控进程树已经确认退出时保留该有限退出保证。若仍有无法确认的进程树，primary 关闭 HTTP、冻结调度并继续持有单实例锁和 cleanup ownership，不能为了满足退出时限而把活进程交给下一实例。

这些修订不改变以下旧不变量：

- `TaskStatus` 仍为 `Queued|Running|Completed|Failed|Cancelled|Interrupted`。
- `ReviewApproved` 只与 `Completed` 共存，`ReviewRejected` 只与 `Failed` 共存。
- 新的 `Completed` 仍只能由 Project 3 的 `finalize_reviewed_task` 质量事务产生。
- 历史 `Completed + Unreviewed` 不因升级获得 approval 或 merge 权限。
- retry 仍创建新 Task、新 attempt、新 worktree、新 provider session 和全新质量证据。
- 已 `ready`、`inconsistent` 或终态任务的 worktree/branch 在 P4-A 中仍不自动删除。

## 4. 术语与核心不变量

### 4.1 术语

- **active task**：已持有全局和仓库 permit、并已 durable 提交 `Queued -> Running` 的任务；permit 直到终态持久化且进程树确认停止后才释放。
- **global permit**：限制当前 primary 中 active task 总数的内存许可。
- **repository permit**：限制同一 `RepositoryCoordinationKey` 下 active task 数量的内存许可。
- **repository control lease**：序列化同 Git 仓库控制面 Git side effect 的短期独占 lease。
- **admission**：Scheduler 决定一个 `Queued` 任务能否获得全部许可并提交 `Running` 的过程。
- **pressure**：空间不足以准入新任务，或空间测量不可用；正常情况下不终止已运行任务。
- **critical pressure**：空间低于紧急阈值，需要停止受影响任务以保护控制面。
- **stop intent**：`user_cancelled|disk_pressure_critical` 两种不可变、首次获胜的停止分类。
- **scheduler projection**：由任务、许可、服务状态、仓库 lease 和 storage 状态计算出的进程内当前视图。

### 4.2 核心不变量

1. `TaskStatus` 不扩展；排队原因、storage 状态、permit、lease、pause 和 stop-in-progress 不能编码成新生命周期状态。
2. 全局 active 数始终 `<= max_concurrent_tasks`。
3. 同一 common Git directory 平台对象身份的 active 数始终 `<= max_concurrent_tasks_per_repository`；持久 `git_identity_key` 是解析该进程内协调身份的稳定种子。
4. `repository_id` 不是 Git 协调键；同 Git 仓库中的多个 Cargo workspace 或路径 alias 登记共享 permit 和 control lease。
5. 等待仓库 control lease 的任务保持 `Queued`，不占 active permit。
6. 全局 permit、仓库 permit、control lease 和 fresh storage admission 全部取得后，任务才能 durable 提交 `Queued -> Running + task.started`。
7. 只有 claim 已知未提交、Task 仍为 `Queued`/终态或精确校正证明本实例不再拥有它时，才能释放 provisional permits/lease；提交结果未知时必须保留资源并校正，不能把 `Busy|Closed` 等同于“未执行”。
8. permit 在 runner 返回后不能立即释放；必须等待对应终态事务 commit，且全部已启动进程树确认停止。
9. worktree `ready` durable 提交后立即释放 repository control lease；Planner/Executor/Reviewer 正常运行期间不持有该 lease。
10. StoreWriter、SQLite mutation、StorageMonitor 和 runtime 回调不得反向获取 repository control lease。
11. 同仓库任务保持 `(created_at, task_id)` 顺序；跨仓库可以跳过暂时不可运行的较老任务。
12. 排队原因由服务端计算且只有一个；React 不从 Task 列表、ServiceState 或本地计数自行推导。
13. 普通 storage pressure 只阻止新 admission；critical pressure 才发出系统 stop intent。
14. 用户取消和磁盘 critical 中，TaskManager mailbox 为该 Task 首次接受并分配 mutation sequence 的 intent 固定进程内 winner；后续 intent 不覆盖。进程崩溃后只恢复已经 durable commit 的 winner。
15. 更早 sequence 的 finalization 已进入不可中断 SQLite 事务并先提交时 reviewed 终态获胜；stop intent sequence 先被接受时，迟到的 Approved/Rejected/普通 runner 终态不得越过它，intent commit 后也不得覆盖。
16. `task_stop_intents` 是持久控制事实，但不产生新的 task event kind；对应终态仍使用现有 `task.cancelled` 或 `task.failed`。
17. queue reason、scheduler generation、permit、lease、storage sample 和 Cargo jobs 不写 SQLite。
18. P4-A 不自动删除、移动、修复或回收 worktree、branch、Git admin metadata、review evidence 或历史事件。

## 5. 架构与所有权

现有 crate 依赖方向保持不变。P4-A 在 `coding-agent-app` 中扩展调度和资源协调，不把 Git、磁盘或 HTTP 引入 `coding-agent-domain`：

```text
Axum API
  -> ApplicationBackend
      -> TaskManager / Scheduler
          -> StoreWriter -> SQLite
          -> RepositoryControlCoordinator
              -> WorktreeProvisioner / typed Git runtime
          -> StorageMonitor
          -> CodingAgentRunner
              -> one worktree / task session / budget ledger
              -> MultiRoleOrchestrator
      -> SchedulerState publisher -> Bootstrap / SSE
```

- `coding-agent-domain` 保持 Task、readiness、事件和质量证据，不拥有动态排队原因。
- `coding-agent-store` 提供 v4 migration、队列原子准入、stop intent 和分场景恢复事务。
- `coding-agent-runtime` 提供卷身份/空间采样 port、common Git capability 身份和固定 Cargo jobs 参数。
- `coding-agent-app` 的 `TaskManager` 仍是生产期 claim、用户取消、系统停止和 Task 终态的唯一所有者；第 16.3 节 startup recovery 是明确的 pre-actor 例外。
- `RepositoryControlCoordinator` 只拥有 app 内 keyed lease，不直接改变 Task 或 artifact。
- `StorageMonitor` 只产生 typed observation/admission/critical 通知，不直接写 Task。
- `StoreWriter` 继续串行化所有生产期 SQLite mutation；启动 migration、artifact reconciliation 和冷恢复是 single-instance、dispatcher/TaskManager/Web Ready 之前唯一允许的直接 Store 例外。
- `coding-agent-api` 和 `web` 消费 typed scheduler DTO，不复制调度规则。

TaskManager mailbox handler 不得 `await` 一个可能阻塞的普通 StoreWriter 请求。它先为每个 Task 分配单调 mutation sequence、记录 typed pending，再把操作交给 writer；completion 作为新的 mailbox message 返回。这样高优先级安全通道可以在普通写等待期间继续设置 safety latch 和 cancellation token，同时同一 Task 的持久顺序仍由 sequence barrier 保证。

锁顺序固定为：

```text
TaskManager actor decision
  -> reserve in-memory global/repository permit
  -> try_acquire repository control lease
  -> StoreWriter Task claim transaction and commit
  -> runtime HEAD validation / deterministic artifact preparation
  -> StoreWriter artifact-reserved transaction and commit
  -> runtime Git side effect
  -> StoreWriter artifact-ready/inconsistent transaction and commit
```

SQLite transaction 不能跨 runtime Git side effect 保持打开。StoreWriter/backend 操作不能等待 TaskManager mailbox、permit 或 repository lease。runtime 回调也不能在持有内部文件/Git mutex 时反向调用 Scheduler。任何违反锁顺序的实现视为不变量错误。

## 6. 启动配置与派生参数

### 6.1 `runtime.json`

配置文件固定为应用私有数据目录中的 `runtime.json`，与含 secret 的 `provider.json` 分离：

```json
{
  "schema_version": 1,
  "max_concurrent_tasks": 2,
  "max_concurrent_tasks_per_repository": 2,
  "max_queued_tasks": 32,
  "storage": {
    "data_control_reserve_bytes": 2147483648,
    "data_task_reservation_bytes": 2147483648
  }
}
```

规则：

- 文件缺失时使用上述默认值。
- 文件存在时必须是 exact object；缺字段、未知字段、重复 JSON key、错误类型、未知 `schema_version` 或尾随内容均无效。
- `max_concurrent_tasks` 为 `1..=4`。
- `max_concurrent_tasks_per_repository` 为 `1..=4`，且不得大于全局上限。
- `max_queued_tasks` 为 `1..=256`。
- 两个 storage byte 值必须是非零 `u64`；按最大 active 数计算时所有乘加必须 checked，溢出即无效。
- 文件必须是普通文件而非 symlink/reparse-point 替身，并复用私有配置的 owner-only 权限检查。
- 文件存在但内容或权限无效时，启动以稳定 `RUNTIME_CONFIG_INVALID` 失败；不能静默回退默认值。
- 配置只在 primary 启动时读取，不热重载；修改后需要重启。
- P4-A 不提供 Web 设置页，也不通过 API 回显本地配置路径。

Bootstrap/Scheduler 只暴露生效后的非秘密限制，不暴露配置文件内容或路径。测试必须显式注入配置，不能依赖开发机 CPU 或磁盘。

### 6.2 Cargo jobs

启动时计算一次：

```text
cargo_jobs_per_task =
  max(1, min(8, available_parallelism / max_concurrent_tasks))
```

除法使用整数向下取整；`available_parallelism` 获取失败时使用 `1`。计算使用配置的全局上限，而不是瞬时 active 数，因此一次进程运行中保持稳定。

所有第一方 `cargo check` 和 `cargo test` 调用都由 runtime 注入 `--jobs=<cargo_jobs_per_task>`。模型不能提供、覆盖或删除该参数。不设置 `RUST_TEST_THREADS`，不新增 provider pool 或 command pool；每个 Task 内部的 provider/工具批次继续串行，不同 Task 可以并发。

## 7. Scheduler 状态模型与公平性

### 7.1 权威输入

Scheduler 只使用以下 typed 输入：

- SQLite 中当前 `Queued` Task 及其 Repository 的内部 `git_identity_key`，以及 runtime 已认证的 `RepositoryCoordinationKey` 映射。
- SQLite 中当前 Running Task 的 durable stop-intent 行。
- TaskManager 当前持有的 global/repository permits。
- TaskManager 当前每 Task 的 pending mutation sequence 和本地 safety latch；只有 durable intent 才进入公开 `stopping_tasks`。
- `RepositoryControlCoordinator` 当前 lease/poison 状态。
- ServiceState/mutation gate 与 scheduler-local freeze 状态。
- StorageMonitor 的最新 logical-scope 状态。
- 启动时固定 RuntimeConfig。

Scheduler 不读取 React 状态，不从日志或 task event 文本反解析，也不把 queue reason 写回 Task。

Stop-intent commit receipt 到达 TaskManager 后才推进 Scheduler generation/publish；wake 或 actor completion 丢失时，下次 rescan 从 SQLite 重新加载 durable intents。TaskManager 重启与第 16.3 节冷启动也从同一表重建，不能只信内存通知。

### 7.2 候选顺序

全部 `Queued` Task 按 `(created_at ASC, task_id ASC)` 排序。一次扫描选择“最早可运行”任务：

1. 若某 Task 的同 `RepositoryCoordinationKey` 下仍有更早的 `Queued` Task，本次不能越过它。
2. 某仓库因 active 上限、control lease 或自身 Git 卷压力暂不可运行时，可以继续扫描其他仓库。
3. 每成功 claim 一个任务后立即更新虚拟/实际 permit 计数，再继续扫描，直到全局容量用尽或无 eligible Task。
4. 不使用随机化、优先级分数、用户可编辑权重、队列位置或 ETA。

同一仓库的 worktree provisioning 因 control lease 串行，但第一个 worktree durable `ready` 后可以释放 lease，让第二个完成 provisioning；随后两个角色循环可真正重叠运行。

### 7.3 排队原因

每个 `Queued` Task 只有一个服务端原因，优先级固定为：

1. `service_paused`
2. `storage_pressure`
3. `global_capacity`
4. `repository_capacity`
5. `repository_control_busy`

含义：

- `service_paused`：ServiceState 非 Ready、mutation gate 关闭，或 Scheduler 因不变量/校正/进程清理而全局冻结。
- `storage_pressure`：候选所需任一 logical scope 为 pressure、critical 或 unavailable。
- `global_capacity`：全局 active 已达上限。
- `repository_capacity`：同 `RepositoryCoordinationKey` active 已达上限。
- `repository_control_busy`：容量可用但 control lease 正被持有、poison 或等待校正。

原因是当前投影，可以在 Task 仍为 `Queued` 时变化。Scheduler 在以下时机重扫并发布语义变化：

- 新任务或 retry durable 创建；
- Task durable started/terminal/cancelled；
- permit 释放；
- repository control lease clean release、poison 或校正完成；
- service pause/resume；
- storage scope 进入或退出 pressure/critical/unavailable。

普通空间采样数值变化但分类、队列原因和容量不变时，不推进 scheduler generation。

## 8. Claim 线性化与 permit 生命周期

### 8.1 Claim

对选中的 Task：

1. 验证 service 和 storage observation 仍可准入；准入使用不超过 5 秒的新鲜样本。
2. 在 TaskManager actor 内为 global 和 `RepositoryCoordinationKey` 预留 permit。
3. 非阻塞 `try_acquire` repository control lease；失败立即释放预留 permit，Task 保持 `Queued`。
4. 通过 StoreWriter 条件提交 `Queued -> Running + task.started`。
5. claim receipt 必须区分 `Applied|ExistingApplied|KnownNotApplied|OutcomeUnknown|InvariantConflict`，不能把 channel `Closed`、deadline 或连接丢失压扁成普通 Conflict。
6. `Applied|ExistingApplied` 只证明 durable claim；先把 permits/lease 放入 `RunContext` 并进入统一 `adopt_and_maybe_launch`，不能从 Store callback 直接 spawn。
7. `KnownNotApplied` 且权威 Task 仍为 `Queued` 时释放 provisional resources 并重扫；权威 Task 已终态时同样释放。真正的不变量冲突保持 frozen。
8. `OutcomeUnknown` 时不启动 runner，也不释放 global/repository permit 或 control lease；冻结该 Task/identity，并通过只读 Store 查询 Task、`last_event_id` 和精确 `task.started` receipt。
9. 校正结果为 `Queued` 且不存在 started receipt 时，证明未提交并释放/重试；为 `Running` 且当前 `last_event_id`、event kind/payload、`started_at` 与本 claim 精确一致时，同样进入 `adopt_and_maybe_launch`；为终态时释放，任何 partial/mismatched tuple 都保持 frozen。
10. `adopt_and_maybe_launch` 在仍持有原 resources 的前提下接管 durable ownership，建立 `RunContext`/`ActiveSafetyRegistry`，随后在同一 Task sequence 下重新检查 ServiceState、mutation gate、shutdown/quiescing、critical safety latch、cancellation token 和 accepted/durable stop winner。只有 gate/服务仍开放、没有 stop/latch/cancel 且 storage 不为 critical 时才启动 runner；普通 pressure 不终止已 active Task。
11. 若统一复检时已有 stop 或 shutdown，只接管 ownership 并按相应路径处理“尚无子进程”的空进程树，再提交 stop/Interrupted 终态，绝不在关闭后启动新副作用。Store 仍不可读时继续保留 resources，既不启动也不释放，直到同一 typed pending 得到确定结果。

Task 的 active 线性化点是 `task.started` 事务 commit，不是 semaphore acquire 或 future spawn。Scheduler 投影携带该 event 的因果水位，客户端不能先看到 active 移动再看到 lifecycle event。

### 8.2 Permit 释放

正常 runner outcome、panic、用户取消、磁盘停止、shutdown 和 store-degraded 路径都遵守：

1. 停止/等待所有已启动 provider/runtime/command 过程。
2. 确认整棵子进程树已经退出。
3. 通过 StoreWriter durable 提交规定终态和 lifecycle event。
4. Scheduler 记录终态 event 水位并更新 projection。
5. 释放 repository/global permits并触发重扫。

如果进程树清理失败或无法确认，不能写一个假装安全结束的普通终态、释放 permits 或启动替代任务。Scheduler 进入 `service_paused`，保留受影响 permits，并继续有界清理/诊断；无法界定影响仓库时全局冻结。

每个受控子进程树还持有一个位于私有 runtime 目录、可跨 primary 崩溃探测的 process-liveness sentinel：Unix 使用随协议内子孙继承、直到最后一个进程退出才释放的 OS file-description lock；Windows 继续以 `KILL_ON_JOB_CLOSE` Job Object 为主，并用等价的不可伪造 handle/file-lock probe 作为启动校验。主动关闭/改写内部 handle 与主动逃逸 process group 一样属于既有恶意代码非目标；正常 protocol 内子孙必须继承它。

sentinel 名称只含随机 instance/task identity，不含路径或 prompt。正常清理在确认树退出并取得独占 probe 后删除它；启动时也只能删除已经成功取得独占 probe 的 stale sentinel，绝不删除仍被任一旧进程持有的文件。

优雅退出预算耗尽但 sentinel/Job 仍不能证明退出时，primary 关闭 listener 并进入 failsafe cleanup 模式，继续持有单实例锁、process handles、permits 和 repository ownership，直到确认清理完成；它不会自愿退出。若 primary 被外部强杀，下次实例必须先探测遗留 sentinel：仍被持有时只做本地启动诊断、不得执行 Running recovery、开放 Web Ready 或准入新任务；释放后才继续第 16.3 节。`PROCESS_TREE_CLEANUP_FAILED` 先是脱敏诊断；只有没有 winning stop intent 且进程树最终确认结束时，才可成为 TaskFailure。已有 stop intent 时，cleanup failure 永不覆盖其最终分类。

## 9. Git 身份与 RepositoryControlCoordinator

### 9.1 协调键

Repository Store 已持久化从 canonical Git root 派生的 `git_identity_key`，Windows 使用既有大小写/路径规范化，Unix 使用 canonical 路径。它是稳定的 durable lookup seed，不是最终的 OS 对象身份。启动/登记时 runtime 必须从 authenticated common Git directory capability 取得平台 `DirectoryIdentityMarker`；per-repository permit 和 control lease 使用该 marker 对应的进程内 `RepositoryCoordinationKey`，不使用 `repository_id`、display name、selected path 或 Cargo workspace root。

同一 `git_identity_key` 每次只能解析到一个仍匹配的对象 marker；同一 marker 被多个 path key、SUBST/bind mount 或 Cargo workspace alias 命中时必须合并为一个 coordination key。路径相同但对象 marker 改变时 poison；平台无法可靠取得/比较 marker 时 fail closed，不退回 path-only 并发。`git_identity_key` 和对象 marker 都不进入公开 DTO。

runtime 在每次 side effect 前后继续重验 durable path key 到 live coordination key 的映射。路径或对象身份不一致时 fail closed，不能把新对象当作旧仓库继续操作。

### 9.2 Lease 范围

`RepositoryControlCoordinator` 是当前 primary 内的 keyed async coordinator。Scheduler 只做非阻塞获取，lease 覆盖：

1. 重验登记 Git root/common Git directory 和 committed HEAD。
2. 确定 deterministic attempt branch/path/base identity。
3. StoreWriter durable 写入 artifact `reserved`。
4. 执行 worktree Git side effect。
5. 重验 branch、path、HEAD、admin record、common-dir 和 Cargo workspace。
6. StoreWriter durable 写入 `ready` 或有证据的 `inconsistent`。
7. clean release lease。

未来 P4-B merge/cleanup 必须复用同一 coordinator 和锁顺序，但 P4-A 不实现这些动作。Agent 角色运行、普通 worktree 内 Git status/diff 和 Cargo 不持有 common control lease。

### 9.3 外部竞争与 lease poison

Coordinator 不锁外部 Git。若验证发现 branch、HEAD、admin metadata、common directory 或 worktree identity 被外部改变：

- 不覆盖、删除或猜测修复对象；
- 持久化能够证明的 artifact 状态；
- 将当前 coordination key 及其全部 `git_identity_key` alias 标记为 poison；
- 阻止该仓库后续 admission；
- 运行只读、身份约束的 reconciliation。

lease 没有强制过期时间。Git 子进程可能仍在运行时不能因超时把 lease 交给下一操作。lease 异常 drop、ready/inconsistent 持久化失败或进程树状态未知都先 poison，再校正；只有 durable、可证明的安全状态才能 clean release。

## 10. Worktree reservation、观察与校正

Project 2 的 `reserved -> ready|inconsistent` 状态机保持不变，P4-A 只把它放入 repository control lease：

| 观察 | artifact 结果 | Task/调度动作 |
|---|---|---|
| reservation 前失败 | 无新 artifact | release lease；按稳定 runner failure 结束 |
| 已 reserved，确认无任何 Git side effect | `inconsistent + WORKTREE_RESERVATION_ABANDONED` | 保留审计，不删除对象 |
| branch/path/admin/base 全部匹配且 worktree ready | `ready` | durable 后 release lease，继续角色循环 |
| 确认 partial、身份或内容矛盾 | `inconsistent + WORKTREE_STATE_INCONSISTENT` | 保留现场，Task 失败 |
| I/O timeout、卷卸载、仓库暂时不可达，无法形成反证 | 保持 `reserved` | identity 保持 busy/poison，稍后重试 |

“无法观察”不能被永久写成 `inconsistent`。只有 absent、partial 或 identity mismatch 等正向证据才能改变 artifact 状态。移动盘、网络卷或权限暂时失败时保留 reservation，Scheduler 显示相应仓库 `repository_control_busy`；若 startup 无法完成任何 required reconciliation，则不开放 Web Ready，错误只通过本地启动诊断/日志呈现。P4-A 不借用 `store_degraded` 表示非 Store 的启动校正状态，也不新增第四个 ServiceState。

冷启动在开放 Scheduler 前按 `(repository git identity, artifact created_at, task_id)` 校正全部 `reserved` 行。每个 identity 内串行，不同 identity 可以在有界启动协调器中并行；P4-A 不自动清理。校正使用新的 bounded token，不复用已经取消的 runner token。

## 11. 磁盘卷模型与采样

### 11.1 Logical scopes

P4-A 监控：

- **data**：`PlatformPaths.data_dir` 所在卷；SQLite、attempt worktree 和每个 worktree 的 Cargo `target` 位于该范围。
- **repository Git**：每个登记仓库 common Git directory 所在卷；branch/ref 和 `.git/worktrees` admin metadata 位于该范围。
- **runtime**：`PlatformPaths.runtime_dir` 所在卷；应用 temp、`TEMP`/`TMPDIR` 等受控临时文件位于该范围。

相同物理卷只采样一次。Windows 使用稳定 volume identity，Unix 使用 mount/device identity；logical scope 只引用采样结果。若 data、runtime 和一个或多个 Git scope 重合，必须同时满足所有适用谓词，但空间预留不重复相加。

空间值使用“当前 OS 用户可用空间”，而不是包含保留块的总 free。所有字节计算使用 checked integer。公开 DTO 不暴露卷 ID、mount path、绝对路径或瞬时 raw byte 数。

### 11.2 采样器

- active 或 queued Task 存在时，每 5 秒触发一次采样。
- admission 前要求相关样本年龄 `<= 5s`；更旧时先刷新。
- 每个物理卷最多一个 in-flight 查询，不能因慢卷堆积线程/future。
- 单次查询预算为 2 秒；timeout、卸载、权限错误或不支持均得到 `unavailable`。
- unavailable 按 pressure 阻止新 admission，但单次测量失败不直接停止 running Task。
- 采样进入 critical 时立即通知 TaskManager 高优先级通道。
- raw sample 仅用于内部判定和脱敏诊断；Scheduler generation 只随分类/原因变化。

### 11.3 Admission 阈值

对一个候选 Task，data scope 要求：

```text
available_data_bytes >=
  data_control_reserve_bytes
  + data_task_reservation_bytes * (active_task_count + 1)
```

`+1` 是候选本身。Git 和 runtime scope 各要求：

```text
available_bytes >= 256 MiB
```

同物理卷重合时取所有适用要求中最严格的一个，不把 256 MiB 再加到 data 公式。测量等于阈值时允许 admission；低于阈值为 pressure。

## 12. Storage 状态、滞回与 critical stop

### 12.1 状态

每个 scope 的公开状态为：

- `normal`
- `pressure`
- `critical`
- `unavailable`

所有 logical scopes 的总状态归并顺序固定为 `critical > unavailable > pressure > normal`，StorageMonitor、Scheduler、Bootstrap/SSE validator 和 React 必须使用同一纯函数。

从 normal 进入 pressure/critical 不延迟。退出 pressure 需要两次成功样本，间隔至少 5 秒，且：

- data 满足“当前 next candidate 阈值 + 512 MiB”；
- Git/runtime 满足 `256 MiB + 64 MiB`。

data 的“next candidate 阈值”即第 11.3 节公式，其中任务份数固定为 `min(max_concurrent_tasks, active_task_count + 1)`；即使当前没有 Queued Task 也使用该假想候选，active 已满时不额外计算第 `max+1` 份。

采样失败会重置退出计数。critical 或 unavailable 后不能用单个正常样本立即恢复 admission。

### 12.2 Critical 阈值与影响集合

critical 固定为：

- data `< 512 MiB`；
- repository Git 或 runtime `< 64 MiB`。

等于 critical 阈值不进入 critical。影响任务集合：

- data critical：全部 active Task；
- runtime critical：全部 active Task；
- 某 Git 卷 critical：所有 Git identity 位于该物理卷的 active Task；
- scope 重合：取任务并集，每个 Task 最多提交一次 stop intent。

### 12.3 紧急停止

TaskManager 为最多四个 active Task 维护一个有界 `ActiveSafetyRegistry`，每项包含 Task/coordination/volume 归属、原子 safety latch 和共享 cancellation token。注册项在 claim adoption 后、runner spawn 前发布，只有进程树确认退出后才移除；发布时必须立即检查当前 critical scopes，避免采样通知与新 runner 之间漏停。StorageMonitor 不直接写 Task，而是先通过该 registry 触发安全动作，再向 TaskManager 的高优先级通道提交 `disk_pressure_critical`：

1. StorageMonitor 先设置进程内 out-of-band safety latch；这一步不等待 TaskManager actor 或 StoreWriter，并立即触发受影响 Task 的 cancellation token/进程树 kill。
2. TaskManager 在高优先级 mailbox 中按 Task 接受首个 intent、分配 mutation sequence，并把受影响 Task 合并成一次 urgent batch。
3. 从收到一批 critical observation 起，整批共享 1 秒 persistence 预算，不是每 Task 各获得 1 秒；不得因任务数线性放大。
4. StoreWriter 当前已经进入的 SQLite transaction 不能被中断；其后 urgent batch 可以越过尚未开始的其他 Task 低优先级请求，但不能越过同一 Task 更早的 sequence、已经开始的 quality finalization 或任何 in-flight transaction。
5. 若预算内无法确认 durable，kill 仍继续保护磁盘，同时进入 Store degraded/frozen；只有真正 commit 的 intent 才进入公开 `stopping_tasks`，不得向 UI 声称本地 safety latch 已落库。
6. 进程树确认退出后，durable disk intent 固定得到 `Failed + DISK_PRESSURE_CRITICAL + retryable=true`。
7. 若 intent 未能持久化且进程随后崩溃，冷启动只信数据库，可能按普通 `Interrupted` 恢复。

TaskManager 的 first-accepted sequence 是进程内 intent winner。若更早的 user cancel 正在等待 Store，critical safety latch 仍可立即 kill，但不能把分类改成 disk；若更早 finalization 已经进入不可中断 Store transaction，则该事务可以先 commit 并成为 reviewed 终态。已知回滚的 mutation 才允许同一 Task 的下一 sequence 继续；commit-before-reply 的未知结果必须保留原 sequence 并查询校正。

urgent batch 最多包含四个 Task，并在当前 in-flight transaction 结束后使用一个 `BEGIN IMMEDIATE` 按 canonical task UUID 顺序处理。每项 query-first 返回 `Applied|Existing|TerminalWon|IntentConflict`；已 reviewed terminal 或既有其他 intent 不阻止同一 transaction 处理其余 Task。事务 commit 后返回逐 Task receipt，任何数据库级失败则整批回滚并进入上述 1 秒预算失败路径。

critical stop 不伪装成用户取消，不自动删除 worktree/branch，也不尝试通过清理历史释放空间。实际 artifact 核算和清理属于 P4-C。

## 13. 资源声明与非目标

已有 per-task command timeout、输出上限、provider/context/step budget、角色轮数和事件 payload bounds 全部保持。全局 active cap 把这些最坏成本限制为最多四份并存；P4-A 不把它们合并成跨任务共享 token budget。

P4-A 能承诺：

- active Task 数量有硬上限；
- 同 Git 仓库 active 数量有硬上限；
- Task 内第一方 Cargo jobs 有固定上限；
- 第一方命令 timeout、输出和进程树清理仍受控；
- managed volumes 在 admission 时满足保守空间条件。

P4-A 不能承诺：

- CPU 百分比、RSS、线程数、测试进程数或 provider 请求速率的 OS 硬限制；
- Cargo/test/build script 不写 managed roots 之外的位置；
- 外部程序不消耗磁盘或修改 Git；
- OS 在 admission 后真正预留了配置字节。

UI 和文档必须使用“受控并发/准入”，不能使用“sandboxed resources”或“磁盘硬配额”。

## 14. 停止意图、取消与终态竞态

### 14.1 类型与线性化

每个 Running Task 的 stop intent 为：

```text
none
user_cancelled
disk_pressure_critical
```

TaskManager mailbox 串行决定候选顺序。首个被接受并分配 per-task mutation sequence 的 intent 在本进程内固定 winner；StoreWriter 必须按该 sequence 持久化，不能让 urgent channel 反转同一 Task 的先后。commit 后该 winner 成为崩溃可恢复事实。同 kind 重放为 Existing，不同 kind 为 typed conflict。Queued cancel 仍在一个既有生命周期事务中直接成为 `Cancelled`，不创建 stop-intent 行。

运行中用户 cancel：

1. TaskManager 接收命令并确认 Task 仍为 Running。
2. TaskManager 接受 `user_cancelled`、固定 winner/sequence，再由 StoreWriter durable 插入。
3. commit 成功后才返回现有 `202 CancellationAcceptedResponse` 并触发 token。
4. 同 intent 网络重放继续返回 accepted；若 Task 已终态，沿用现有 `200 TaskDto`。
5. 若 `disk_pressure_critical` 已获胜，用户 cancel 返回 `409 TASK_STOP_ALREADY_REQUESTED`，不改变分类。

磁盘 critical 使用第 12.3 节的紧急安全例外：持久化失败不能阻止 kill，但不能伪造 durable acceptance。

### 14.2 与 runner/quality 的竞态

stop-intent 插入、`record_review` 和 `finalize_reviewed_task` 都使用 `BEGIN IMMEDIATE` 并在事务内验证对方状态：

- final reviewed transaction 已先进入不可中断 writer transaction 并 commit：`Completed + ReviewApproved` 或 `Failed + ReviewRejected` 获胜，随后 intent insert 发现 terminal 并失败。
- stop intent 已获得更早 mutation sequence 但仍在等待 Store 时：同一 Task 的较晚 review/finalization 停在 sequence barrier，不能抢先进入 transaction。
- stop intent 先 commit：新的 review evidence/finalization 不能提交；Task 最终只能按 intent 分类。
- 普通 runner Approved/Rejected/Failed/Cancelled outcome 在 TaskManager 中先读取 accepted pending winner 和 durable intent；任一存在时都停在 sequence barrier，不能覆盖。
- stop intent 后已经在途的普通 activity/diff/test snapshot 可以在 Task 仍 Running 时完成，但不能清除 intent、创建 readiness 或改变最终分类。
- terminal transaction commit 后收到的 intent、事件或 runner outcome按现有 CAS/幂等规则拒绝。

用户停止：

```text
Running + Unreviewed
  -> Cancelled + Unreviewed
  -> task.cancelled
```

磁盘停止：

```text
Running + Unreviewed
  -> Failed + Unreviewed
  -> failure {
       code: "DISK_PRESSURE_CRITICAL",
       retryable: true
     }
  -> task.failed
```

stop intent 不能创建 `ReviewRejected`，中间 review evidence 保留但不成为 delivery state。

### 14.3 Store degraded 与 PendingDurableResult

`PendingDurableResult` 增加 typed stop-intent/final-stop 操作。结果未知时：

- Scheduler 和 mutation gate冻结；
- 用户请求在 durable receipt 前不报告 accepted；
- critical disk 可先 kill，但保留 typed pending；
- 恢复后按每个 Task 原始线性化顺序幂等重放；
- 已接受 stop intent 必须在 generic interrupt 之前解析；
- conflict 表示不变量问题并保持 frozen，不能猜测 winner。

冷进程重启没有内存 pending，只信 SQLite 中已经 commit 的 intent、quality tuple 和 Task 状态。

### 14.4 Final-stop 原子事务

`finalize_stopped_task(task_id, expected_repository_id, expected_attempt, expected_intent)` 使用 query-first `BEGIN IMMEDIATE`：

1. `Running + Unreviewed`、identity/attempt 精确匹配且存在相同 intent 时，使用同一个 Store-generated UTC timestamp 更新 Task `finished_at`、按 intent 插入现有 terminal event，并在 disk intent 时插入精确 failure tuple；`Task.last_event_id` 指向该 event。
2. 若 Task 已终态，只有 Task status/failure、保留的 immutable intent、terminal event kind/payload/timestamp 和 `last_event_id` 全部组成预期 exact tuple 时才返回 `Existing` 及原 terminal event ID，并重新触发 dispatcher wake。
3. partial tuple、不同终态、不同 failure、不同 intent 或额外 terminal lifecycle event 都返回 typed invariant conflict，不能追加“修复”事件。
4. 只有第 1 项能创建新 terminal event；commit-before-wake、commit-before-reply 和 response loss 的重放都走第 2 项，永远不重复 lifecycle。

进程内调用该事务前必须已经确认进程树退出。若 cleanup 失败且存在 intent，只冻结/诊断，最终成功清理后仍按 intent 完成；`PROCESS_TREE_CLEANUP_FAILED` 不能覆盖 user/disk winner。

## 15. 队列背压与幂等创建

### 15.1 计数

`max_queued_tasks` 是全局上限，只统计 `tasks.status='queued'`。Running 和终态不占 queue slot；不增加 per-repository queued quota，不维护独立持久计数器。

队列计数、幂等查找和插入必须位于同一个 `BEGIN IMMEDIATE` 事务。StoreWriter 串行化只是第二层保护，正确性不能依赖 API 预查或内存计数。

### 15.2 Create

顺序固定为：

1. 按 `client_request_id` 查已存在 Task。
2. repository/prompt 与原 canonical input 相同：即使队列已满也返回 Existing。
3. 同 ID 不同 input：返回既有 `409 IDEMPOTENCY_CONFLICT`。
4. 只有真正的新请求才 `COUNT(*) WHERE status='queued'`。
5. `queued >= max_queued_tasks` 时返回 queue-full，不写 Task、event 或 dispatcher/manager 通知。
6. 未满时原子插入 Task、`task.queued` 和 `last_event_id`。

这样“服务已提交但响应丢失”的原请求重放不会被队列满遮蔽。

### 15.3 Retry

Retry 先验证 source 和查询已有 direct retry child：

- 已有 child 时即使队列已满也返回 Existing。
- source 不可 retry 仍返回 `TASK_NOT_RETRYABLE`。
- 只有首次创建 child attempt 才检查 queue cap。
- 并发 retry 在 `BEGIN IMMEDIATE` 下最多创建一个 child/event。

### 15.4 HTTP

真正的新创建或首次 retry 遇到满队列时：

```json
{
  "code": "TASK_QUEUE_FULL",
  "message": "the task queue is full; retry after capacity becomes available",
  "retryable": true,
  "request_id": "123e4567-e89b-42d3-a456-426614174000",
  "details": {
    "queued_tasks": 32,
    "max_queued_tasks": 32
  }
}
```

- HTTP 为 `429 Too Many Requests`。
- 不使用 `409`，因为请求内容没有冲突。
- 不使用 `503`，因为应用和 Store 仍健康。
- 不发送 `Retry-After`，因为 Scheduler 不承诺 ETA。
- `POST /api/tasks` 和 retry endpoint 的 OpenAPI 都声明 `429`。

React 可在 fresh scheduler snapshot 显示队列已满时禁用全新提交；对于结果未知且保留原 `client_request_id` 的显式重放仍允许点击，因为请求可能已经提交。服务器事务始终是唯一准入权威。

## 16. SQLite v4、恢复与迁移

### 16.1 v4 schema

`0004_concurrent_scheduler.sql` 新增：

```text
task_stop_intents
  task_id        PRIMARY KEY
  repository_id
  attempt
  kind           user_cancelled | disk_pressure_critical
  requested_at
  FOREIGN KEY (task_id, repository_id, attempt)
    -> tasks(id, repository_id, attempt)
```

要求：

- 使用 `STRICT` 或等价的完整 type/CHECK 约束。
- identity、attempt、kind、timestamp 全部 non-null；attempt 为正整数。
- INSERT 只允许当前 Task 为 `Running + Unreviewed`。
- UPDATE/DELETE 由 abort triggers 禁止；另有 `BEFORE INSERT ... WHEN EXISTS(task_id)` abort trigger，阻止 `INSERT OR REPLACE` 先删后插绕过不可变性。
- Store 必须 query-first：同 Task 同 kind返回 Existing，异 kind 返回 typed conflict；禁止 `INSERT OR REPLACE`、`REPLACE` 和会走 UPDATE 的 UPSERT。
- intent 行在 Task 终态后保留作不可变审计。
- Task 从 Running 转终态时，DB trigger/Store 双层保证 user intent 只能对应 Cancelled，disk intent 只能对应指定 Failed tuple。
- quality finalization/record review 必须验证不存在 intent。

同一 migration 增加：

```text
tasks(created_at, id) WHERE status = 'queued'
```

partial index 只优化 Scheduler 顺序和 queue count，不承担正确性。不向 `tasks` 添加 reason、permit、lease、storage 或 Cargo 列。

### 16.2 迁移兼容

验证空库、v1→v4、v2→v4、v3→v4 和当前 v4 重开：

- 不回填任何 stop intent。
- 旧 Task/event/artifact/review/delivery row 不重写、不伪造、不改变 readiness。
- 11 种已持久 task event 仍为 `schema_version=1`。
- v1 跳级正确依次获得 v2/v3/v4 schema。
- 重复 migrate 无变化。
- migration 中途任一步失败时本次 open 的全部未提交版本回滚。
- 升级后 `PRAGMA foreign_key_check` 为空，STRICT/CHECK/trigger corruption tests fail closed。
- 已存在的 `schema_migrations` 必须恰好是从 1 开始、无空洞且最大不超过 4 的连续前缀；未来版本、0/负数或任何 gap 以稳定 `DATABASE_SCHEMA_UNSUPPORTED` 使 startup fail closed，且本次 transaction 不留下 schema 写入。

数据库 migration version `4` 与 wire/event `schema_version=1` 是不同命名空间，不能联动递增。

### 16.3 冷启动恢复

在 single-instance lock 内、dispatcher/StoreWriter/TaskManager/Web Ready 之前：

1. 加载并验证 runtime/provider config 与私有路径，但尚不打开 SQLite。
2. 先探测上一实例所有 process-liveness sentinel；仍被持有时不得打开/迁移 SQLite、执行 Running recovery 或打开 Web，只保留本地诊断并重试 cleanup proof。
3. sentinel 全部可独占后才打开 SQLite，先验证 migration history，再完成 migration。
4. 初始化 RepositoryControlCoordinator，并以 startup-only direct Store 操作校正所有 `reserved` artifact；每次 observation/update 幂等，暂时不可观察时 startup 失败且不伪造 inconsistent。
5. 执行单个 `recover_after_restart` 的 `BEGIN IMMEDIATE`：先验证全部既有 intent/terminal tuple，再按 `(stop requested_at, task_id)` 在同一 transaction 内复用第 14.4 节的 query-first helper 终态化 Running intent（不能嵌套开启 transaction），再按 `(task created_at, task_id)` 把其余 Running 改为 `Interrupted + APP_RESTARTED`，保持全部 Queued 原状态和既有 `task.queued` event。
6. 第 5 步的所有 Task/Failure/event/`last_event_id` 变更全有或全无，并返回 committed high watermark；任何冲突/写失败使 startup 停止，不能开放部分恢复的 Scheduler。
7. 以该 high watermark 启动 EventDispatcher；随后才启动唯一生产 StoreWriter、TaskManager 和 Scheduler。由于此前没有客户端或生产 writer，startup artifact 写不产生 event，而 recovery 产生的 events 已包含在 dispatcher 初始 cursor 中，不存在 missed wake 窗口。
8. 从实际 Running=0、Queued 行、与 Running 关联的 durable stop-intent 行和 fresh storage sample 初始化 Scheduler，完成 Bootstrap 因果校验后才开放 Web Ready/重扫。

若 startup 在 recovery commit 后、收到 receipt 前崩溃，下次 query-first recovery 把 exact terminal tuple 当作 Existing，不追加重复 terminal event。Artifact reconciliation 与 recovery transaction 分开；前者已提交的 observation 更新本身幂等，后者仍严格原子。

如果升级数据库中的 Queued 数已经超过新配置：

- 不删除、中断或重排旧任务；
- available queue capacity 使用 saturating subtraction，结果为 0；
- 新 create/retry 返回 `TASK_QUEUE_FULL`；
- Scheduler 继续消化旧队列，直到低于上限。

### 16.4 优雅关闭与进程内 degraded

优雅关闭和 Store degraded recovery 保持与冷启动不同：

1. Scheduler pause，mutation gate关闭，不再 claim。
2. 等待已进入 gate 的 mutation，并按原线性化顺序持久/重放 typed pending。
3. 已先提交的 reviewed/普通终态保持；对全部仍非终态的 active runner 触发 cancellation，包括已经有 durable stop intent 的 Task，并确认每棵进程树退出。intent persistence 本身不是 terminalization。
4. 只有第 3 步确认退出后，才把 durable stop intents 完成到各自规定终态。
5. 再把其余 `Queued/Running` 改为 `Interrupted`；其中任何 Running 也必须已经在第 3 步确认退出。
6. 释放 permits，flush 到最新 event ID，之后才退出或恢复 Ready。

已确认的 user cancel 不能被 shutdown 改写为 Interrupted；durable disk intent 不能被改写为 user cancel。若 Store 在退出预算内不可写但所有进程树已确认退出，沿用 degraded-shutdown marker 和非零有限退出；下次冷启动只依据已提交事实。若仍有进程树未知，则进入第 8.2 节 failsafe cleanup，不能释放单实例锁或退出。

## 17. API、OpenAPI 与 SSE

### 17.1 Scheduler DTO

Bootstrap 增加 required `scheduler`，同时保留既有顶层 `max_concurrent_tasks` 作为兼容 alias：

```text
SchedulerStateDto {
  schema_version: 1,
  server_instance_id: UUID v4,
  server_started_at: RFC3339 UTC string,
  generation: nonnegative safe integer,
  as_of_event_id: nonnegative safe integer,
  service_state_generation: nonnegative safe integer,
  admission_state: running | paused,
  limits: {
    global: u32,
    per_repository: u32,
    queued: u32,
    cargo_jobs_per_task: u32
  },
  active_task_count: u32,
  queued_task_count: u32,
  queued_tasks: [
    {
      task_id: UUID,
      reason:
        service_paused
        | storage_pressure
        | global_capacity
        | repository_capacity
        | repository_control_busy
    }
  ],
  stopping_tasks: [
    {
      task_id: UUID,
      intent: user_cancelled | disk_pressure_critical
    }
  ],
  storage: {
    state: normal | pressure | critical | unavailable,
    data: {
      state: normal | pressure | critical | unavailable
    },
    runtime: {
      state: normal | pressure | critical | unavailable
    },
    repositories: [
      {
        repository_id: UUID,
        state: normal | pressure | critical | unavailable
      }
    ]
  }
}
```

公开 storage 只暴露 logical categorical state，不暴露路径、物理卷 identity 或每 5 秒变化的 raw free bytes，避免隐私泄露和 SSE 抖动。

`storage.state` 是 logical scopes 的确定性聚合：任一 scope critical 时为 critical；否则任一 scope unavailable 时为 unavailable；否则任一 scope pressure 时为 pressure；其余为 normal。

`admission_state` 不是“此刻一定有空位”：只有且仅有 ServiceState 为 Ready、mutation gate 开放且不存在 scheduler-global freeze 时为 `running`。容量已满、单仓库 busy 或 storage pressure 由各自字段/queue reason 表示，不把顶层 admission 改成 `paused`。

所有 scheduler object 都是 exact object：列出的字段 required、non-null，未知字段拒绝；整数必须是 JSON safe integer 并满足声明的非负/u32 范围，UUID 必须 canonical lowercase hyphenated。`server_instance_id` 复用 single-instance descriptor 中本次 primary 的随机 UUID，是 scheduler epoch 的唯一 identity；`server_started_at` 只用于显示和一致性检查，时钟相同或回拨不能合并两个 epoch。

数组顺序固定：

- `queued_tasks` 按权威 Task `(created_at, task_id)`；
- `stopping_tasks` 按 intent `(requested_at, task_id)`；
- `storage.repositories` 按 canonical repository UUID。

`queued_tasks` 在 Bootstrap 中是完整逻辑投影；新库最多为 `limits.queued<=256`，升级时上限是 startup 捕获的有限 `legacy_queued_count`，之后只能下降或在低于配置上限时重新增长。`stopping_tasks.length<=active_task_count<=4`。repository storage 恰好覆盖 Bootstrap 中每个 Repository；共享同一 Git 卷的 Repository 可以显示相同状态。数据库计数无法表示为 u32 时 startup 以 `DATABASE_PROJECTION_LIMIT_EXCEEDED` fail closed，而不是截断或整数回绕。

交叉约束：

- 顶层 `BootstrapResponse.max_concurrent_tasks == scheduler.limits.global`。
- `scheduler.server_started_at == BootstrapResponse.server_started_at`。
- `scheduler.service_state_generation == BootstrapResponse.service_state_generation`。
- `active_task_count <= limits.global`。
- `queued_task_count == queued_tasks.length`；legacy 超额时允许大于 `limits.queued`。
- `queued_tasks` task_id 唯一，并精确覆盖 Bootstrap/Snapshot 中当前 Queued Task。
- `stopping_tasks` task_id 唯一，只引用当前 Running Task，并精确覆盖 `task_stop_intents JOIN tasks ON status='running'` 的 durable 行；终态 Task 保留的 intent 只是 immutable audit，不显示为 stopping，本地 safety latch/pending write也不进入该数组。
- repository storage entries按 repository ID 唯一，并精确覆盖 Bootstrap repositories。
- `as_of_event_id` 是该投影已观察到的最后一个 Task membership lifecycle event ID；它必须等于 Bootstrap Store snapshot 中相同定义的 membership watermark，且 `<= BootstrapResponse.latest_event_id`。

Bootstrap 通过 bounded retry join 生成，不能把分别读取的近似快照直接拼接：

1. 捕获本次 `server_instance_id`，读取 ServiceState generation `S1`。
2. 在一个一致 Store read 中取得 repositories、tasks、与 Running 关联的 durable intents、`latest_event_id` 和 membership watermark `M`；终态 intent 另作完整性校验但不进入 Scheduler 集合。
3. 读取 Scheduler projection `Q`，再读 ServiceState `S2`。
4. 只有 `S1==S2==Q.service_state_generation`、`Q.as_of_event_id==M`、Task/intent/repository 集合满足上述 exact 约束时返回。
5. `Q` 落后 `M` 时等待 scheduler watch 追平；`Q` 超前 `M` 或任一集合/ServiceState 在读取中变化时重做 Store snapshot。超过固定启动/请求预算返回现有 exact error envelope 的 `503 BOOTSTRAP_SNAPSHOT_UNAVAILABLE`，不返回内部不一致的 200。

`TaskDto`、`TaskDetailDto`、`CancellationAcceptedResponse` 和已有 TaskEvent payload 不增加动态 scheduler 字段。

### 17.2 SSE control

Scheduler 在内存中仍是完整 logical snapshot，但 SSE wire 使用一个 manifest 加有界 chunk，避免 legacy 大队列形成超大单帧：

```text
SchedulerStateControl {
  schema_version: 1,
  kind: "scheduler.state",
  server_instance_id,
  server_started_at,
  generation,
  as_of_event_id,
  service_state_generation,
  admission_state,
  limits,
  active_task_count,
  queued_task_count,
  stopping_task_count,
  repository_storage_count,
  storage: {
    state,
    data,
    runtime
  },
  item_count,
  chunk_count,
  snapshot_digest: lowercase SHA-256
}

SchedulerStateChunkControl {
  schema_version: 1,
  kind: "scheduler.state.chunk",
  server_instance_id,
  generation,
  snapshot_digest,
  chunk_index,
  chunk_count,
  items: [
    {
      kind: "queued_task",
      task_id,
      reason
    }
    | {
      kind: "stopping_task",
      task_id,
      intent
    }
    | {
      kind: "repository_storage",
      repository_id,
      state
    }
  ]
}
```

规则：

- 两类 frame 都没有 SSE `id`，不推进 task event cursor，不写 `task_events`，也不新增第 12 种持久事件。
- items 的 canonical 顺序为全部 queued、全部 stopping、全部 repository storage，各组沿用第 17.1 节排序；每个 chunk 最多 128 items，manifest/chunk 序列化后各自必须 `<=64 KiB`，否则 server fail closed 并触发 snapshot recovery。
- `item_count` 等于三个 count 之和；`chunk_count=ceil(item_count/128)`，空 items 时为 0；chunk index 从 0 连续到 `chunk_count-1`，各 chunk 重复同一 epoch/generation/digest。
- 三种 item 都是以 `kind` 为 discriminator 的 exact object：只允许上面列出的 required/non-null 字段，未知字段拒绝，各 enum 使用第 17.1 节的 exact lowercase wire 值。
- `snapshot_digest` 是完整 logical `SchedulerStateDto` 按 RFC 8785 JSON Canonicalization Scheme 编码为 UTF-8 bytes 后的 SHA-256 lowercase hex；数组先按本节顺序构造，Rust/TypeScript fixture 必须对同一值产生逐字节相同的 canonical bytes/digest。客户端只在收到全部 chunk、无重复/空洞、计数/排序/digest 全部正确后原子构造 logical DTO；partial generation 从不进入 UI state。
- Rust `SseMessage.oneOf`、OpenAPI、exported `web/openapi.json`、generated TypeScript、runtime validator 和 reducer 必须在同一变更中更新。
- SSE parser 必须在解析 persisted event ID 前识别 `scheduler.state|scheduler.state.chunk`；不能把未知无 ID control 送入 lifecycle recovery loop。

Scheduler projection 使用 full-snapshot/watch 语义。Publisher 只保留一个最新 immutable logical snapshot；每个 SSE connection 同时最多持有一个 in-flight/partial generation，完成后可以继续发送更新 generation，中间已被覆盖的 generation 只 coalesce 到最新值而不在内存排队。已经开始但因更高 task/service 水位失效的分段可以中止并切换最新 generation。长期运行的内存增长因此受当前 durable Task/Repository 集合约束，而不随历史 generation 或慢客户端持续增长。

`generation` 在当前 `server_instance_id` 内从 0 单调增长，仅随公开语义变化。比较规则：

- 同 `server_instance_id` 且 generation 更高：候选新状态。
- generation 相同且 digest/canonical payload 相同：幂等，可清除 reconnect 后的 stale 标记。
- generation 相同但 payload 不同：协议冲突，进入 bootstrap recovery。
- generation 更低：忽略为陈旧。
- `server_instance_id` 不同：丢弃所有 partial/current scheduler state 并重新 bootstrap；时间戳相同也不能复用。

P4-A 的分段只解决当前 scheduler control 的单帧和 generation backlog，不引入历史分页、搜索或制品生命周期；这些仍属于 P4-C。

### 17.3 因果水位与建连

客户端除现有完整 `applied_task_event_id` 外，还跟踪 `applied_membership_event_id`：只在应用 `task.queued|task.started|task.completed|task.failed|task.cancelled|task.interrupted` 时更新。`as_of_event_id` 表示 Scheduler membership 已观察到的最后一个此类 event；普通 activity/plan/diff/test/review event 不改变它。`service_state_generation` 是计算 `service_paused` 时使用的精确 ServiceState generation。

客户端只有在：

```text
applied_membership_event_id == scheduler.as_of_event_id
and applied_service_generation == scheduler.service_state_generation
```

时才能应用完整组装的 control。客户端水位较小时缓存最新合法 generation；水位已经较大或 service generation 已越过时，候选是旧投影，必须丢弃并等待更新，不能因 `>=` 而倒灌旧 membership/service 状态。这样 terminal event 必须先于 active/stopping 移除可见，`task.started` 也必须先于 active 增加可见。

SSE backend 建连顺序：

1. 在任何可能等待的 Store fetch 前订阅 task live、service state 和 scheduler watch。
2. 立即读取并发送当前 `service.state`。之后所有 Store fetch、event replay 和 scheduler chunk 发送都以 `select!`/等价公平轮询继续发送更新后的 service control 与 heartbeat；不能让慢 DB 或大 legacy queue 饿死既有 service 语义。
3. 按客户端 cursor 回放持久 task events并缓冲/补取 live events，drain 到捕获水位并按 ID 发送。
4. 选取最新 scheduler snapshot；只有 task membership 已发送到其 `as_of_event_id` 且最新已发送 service generation 与其精确相等时，才发送 manifest/chunks。service 在分段期间变化时可中止该旧 generation，转向匹配的新 projection。
5. 进入 live merge；persisted task events保持 ID 顺序，service 独立公平，scheduler 始终受 exact 因果门约束。live lag 仍通过 Store refill；无法完成一致分段时发起现有 `stream.reset`/Bootstrap recovery。

`stream.reset` 后客户端重新 bootstrap。Bootstrap 期间收到的更高 scheduler generation 不能被旧 snapshot 覆盖；若 Bootstrap 的 instance/membership/service tuple 与 buffered control 不可比较，丢弃 partial control 并重新订阅。Snapshot 只通过上述 instance/generation/digest/exact-watermark 规则仲裁。

### 17.4 兼容边界

Bootstrap 当前由前端 exact-object 校验，新增 required `scheduler` 不是旧 JavaScript bundle 可透明忽略的变化。P4-A 的兼容承诺是：

- SQLite v1–v3 数据和 task event replay 向后迁移；
- Rust server、embedded React、OpenAPI 和 generated client 在同一发行包原子升级；
- 不承诺旧浏览器 bundle 与新 server 的滚动混用；
- HTML/API 继续 `Cache-Control: no-store`，进程重启后的 session/Bootstrap 重新建立。

既有 `service.state` wire shape 不改变；`service_paused` 是 Scheduler admission 投影，不新增第四个 ServiceState。

## 18. React 工作台

- 顶部或任务列表附近显示 `active_task_count / limits.global`。
- 显示 per-repository active limit、queue usage 和 `cargo_jobs_per_task`，不提供编辑控件。
- Queued Task 显示服务端文案：
  - Waiting for the service
  - Waiting for storage
  - Waiting for global capacity
  - Waiting for repository capacity
  - Waiting for repository coordination
- 不显示数值队列位置或 ETA。
- SSE 断开时保留最后 scheduler snapshot 供查看，但显式标记 stale；不得继续把它当作准入事实。
- 同 `server_instance_id`、同 generation、同 digest/payload 的完整 reconnect projection 可以清除 stale；新 instance 必须 bootstrap。
- Running Task 出现在 `stopping_tasks` 后禁用重复 cancel，并区分 “Stopping — user requested” 与 “Stopping — critical storage pressure”。
- user cancel 和 disk critical 最终分别显示 Cancelled 与 retryable Failed，不混用文案。
- queue-full 新建错误保留原 prompt 和 `client_request_id`；fresh snapshot 恢复容量后允许显式重放。
- 若 legacy 队列超过新上限，UI 如实显示 `queued_task_count > limits.queued`，不删除或隐藏任务。
- UI 不从本地 Task 状态推断 queue reason、storage state、stop winner 或 readiness。

P4-A 不显示 merge、cleanup、artifact size、history search、provider concurrency 或 dynamic runtime settings。

## 19. 稳定错误、诊断与脱敏

新增或正式使用：

- `RUNTIME_CONFIG_INVALID`：启动配置缺陷，非 TaskFailure。
- `DATABASE_SCHEMA_UNSUPPORTED`：migration history 为未来版本、空洞或非法前缀，startup fail closed。
- `DATABASE_PROJECTION_LIMIT_EXCEEDED`：现有行数无法安全投影为公开 u32 计数，startup fail closed。
- `BOOTSTRAP_SNAPSHOT_UNAVAILABLE`：在有界重试内无法取得一致 Store/Service/Scheduler join，HTTP 503。
- `TASK_QUEUE_FULL`：HTTP 429，可重试。
- `TASK_STOP_ALREADY_REQUESTED`：HTTP 409，另一 stop intent 已获胜。
- `DISK_PRESSURE_CRITICAL`：Task Failed，可 retry。
- `WORKTREE_RESERVATION_ABANDONED`：artifact 有 durable reservation但确认没有 side effect。
- `WORKTREE_STATE_INCONSISTENT`：沿用 Project 2 的正向不一致证据。
- `PROCESS_TREE_CLEANUP_FAILED`：进程树未知时先作为冻结诊断；无 stop intent 且最终确认退出后才可成为 TaskFailure。

storage `unavailable` 和普通 pressure 是 Scheduler 状态，不创建 TaskFailure。repository control busy/poison 先保留 Queued；只有实际 attempt claim/runner 已开始且形成稳定失败时才写 TaskFailure。

用户可见 message 固定、短且脱敏。不得包含：

- API key、Authorization、cookie、launcher/session secret；
- provider raw body、模型 reasoning 或完整命令输出；
- common Git directory、worktree、runtime、mount、UNC 或用户主目录绝对路径；
- 原始 prompt、diff 或仓库文件内容。

内部结构化日志可以记录 Task/Repository UUID、scheduler generation、queue reason、配置上限、available byte 数和散列/匿名 volume identity，但不能记录秘密或完整用户路径。所有并发日志必须带 task_id，避免多任务交错后无法归属。

## 20. 测试策略

### 20.1 配置与纯 Scheduler

- runtime.json 缺失默认、exact parse、未知/重复字段、版本、权限、边界、溢出和无 fallback。
- global/per-repository/queue limit 的全部最小最大组合。
- Cargo jobs 在 CPU 1、2、4、8、64 和获取失败时的确定值。
- `(created_at, task_id)` tie-break、跨仓库跳过和同仓库不超车。
- 五种 queue reason 的单项与组合优先级。
- generation 只随语义变化，不因相同 storage sample 抖动。
- claim `KnownNotApplied|OutcomeUnknown`、commit-before-reply adoption、adoption 与 shutdown/stop/critical latch 竞态、permit 保留/释放、double release、actor panic 和重复通知。

### 20.2 Store、迁移与恢复

- 空库、v1/v2/v3→v4、重复 migrate、未来版本/空洞拒绝、foreign keys、STRICT、triggers 和逐 SQL 故障回滚。
- stop intent 同值重放、异值冲突、terminal insert 拒绝、`INSERT OR REPLACE`/UPSERT 绕过拒绝和 raw corruption fail closed。
- intent、Task、failure、last_event_id 和 lifecycle event 的全事务故障注入；final-stop commit-before-wake/reply 重放返回原 event ID。
- create/retry 在并发下恰好填满 queue slot，不超卖。
- 满队列时 Existing 优先、IDEMPOTENCY_CONFLICT 优先、首次新请求 429 且零副作用。
- 单个原子 cold-recovery transaction 的 user intent→Cancelled、disk intent→指定 Failed、其余 Running→Interrupted、Queued 保留；任一点失败全回滚。
- startup artifact reconciliation、recovery high watermark、dispatcher 初始 cursor 和生产 StoreWriter 启动顺序不漏发/重复事件。
- graceful/degraded 的 pending replay、stop intent 优先和其余 Queued/Running→Interrupted。
- legacy 超额队列不改写并能逐步排空。
- quality evidence/readiness/final event 原子性在 stop-intent 竞态下不退化。

### 20.3 Repository control 与 worktree

- 同 Git root 不同 Repository/Cargo workspace 共享 permit/lease。
- Windows case/SUBST/path alias、Unix canonical/bind-mount alias 和同对象不同 durable key 不会绕过协调；对象 identity 不可用时 fail closed。
- 不同 Git identity 的 worktree provisioning 可独立进展。
- 同 identity 的 reservation/Git/admin mutation严格串行，ready 后角色执行重叠。
- reservation 前、reserved 后、Git spawn 前后、partial checkout、ready 写前后、wake 丢失和 lease abnormal drop。
- absent→abandoned、exact ready→ready、partial/mismatch→inconsistent、I/O unavailable→保留 reserved。
- 外部 branch/admin/HEAD/common-dir 替换 fail closed且不清理未知对象。
- Store durable ready/inconsistent 失败时 lease poison，不提前释放。

### 20.4 StorageMonitor

- Windows/Unix volume identity、同卷 dedup、不同卷独立判定。
- 当前用户 available 空间、阈值等号、checked arithmetic 和 sample freshness。
- 5 秒周期、2 秒 timeout、每卷单 in-flight 和慢/卸载/权限失败。
- normal→pressure、两次滞回恢复、失败样本重置滞回。
- data/Git/runtime critical 阈值和重合卷任务并集。
- ordinary pressure 不取消 running；critical 每 Task 只提交一个 intent。
- out-of-band safety latch、整批 1 秒 budget、actor 正等待普通 write 时仍 kill、同 Task sequence 不反转、StoreWriter before/after commit、超过预算进入 degraded。
- 无自动 artifact scan、删除或清理。

### 20.5 TaskManager 竞态与进程

- global 2、repository 2 下同仓库两个真实 Runner 同时处于角色执行；此时第三个及其他仓库 Task 均为更高优先级的 global_capacity。
- 另以 global 至少 3、repository 2 证明第三个同仓库 Task 为 repository_capacity，且另仓库 eligible Task 可跳过运行。
- user-first、disk-first、相同 intent replay、不同 intent conflict。
- cancel-vs-Approved/Rejected/Failed 的两个事务提交顺序。
- stop intent 后 late activity/diff/test 可有界收尾但不能改变终态。
- shutdown、store degraded、runner panic、TaskManager close 和 process cleanup failure。
- Windows Job Object、Unix process group 下子孙进程确认退出前不释放 permit。
- cleanup 超过 shutdown budget 时 primary 保留 single-instance lock；外部强杀后的 process-liveness sentinel 阻止新实例 recovery/admission，直到旧树退出。

### 20.6 API、SSE 与 React

- Bootstrap exact shape、随机 `server_instance_id`、bounded exact join 和所有交叉约束。
- 顶层 legacy max 与 scheduler limit 一致。
- `scheduler.state` manifest/chunks 无 id、无持久 row、不会推进 event cursor；每帧 `<=64 KiB`、每 chunk `<=128` items。
- chunk missing/duplicate/out-of-order、digest mismatch、新 generation 抢占和 legacy 超额队列完整原子组装。
- membership event/service generation 精确相等门；旧 projection 不因客户端 `>=` 水位而倒灌，started/terminal 先于 scheduler membership。
- service control 在慢 replay/大 scheduler 分段期间仍立即且公平；bootstrap→subscribe、replay、live、lag、stream.reset、同 generation replay、新 instance 和协议冲突恢复。
- Rust OpenAPI、exported JSON、generated TS、runtime validator 无 drift。
- stale UI、五种原因、capacity、queue-full retry、user/disk stopping 和 legacy over-limit。

### 20.7 Offline E2E 与压力

- 临时真实 Git/Cargo 仓库 + scripted fake provider，证明同仓库两个独立 worktree 的角色循环实际重叠。
- worktree control phase 串行且原工作目录 dirty 内容不进入任何 Task。
- 两个仓库在一个仓库受 control/storage 阻塞时仍公平推进。
- 浏览器断开/重连时后台继续且 scheduler 恢复。
- 强杀场景同时包含 Running、Queued 和 durable stop-intent，重启后分别得到规定结果。
- 长时间多轮 create/start/finish、满队列背压、legacy 大队列分段、输出洪泛和 StoreWriter lag 不泄漏 permit或随历史 generation/慢客户端无限增长内存。
- storage fake 覆盖 pressure/critical/hysteresis；真实卷测试只使用隔离临时目录，不填满开发机磁盘。

所有默认测试离线、确定性且不访问真实 provider。P4-A 不消耗新的真实 provider 尝试授权。

## 21. 验收门

P4-A 只有在以下全部成立时完成：

1. 用户复核并批准本规格，且实现前另有逐步 TDD 计划。
2. production runtime.json 默认全局/同仓库并发为 2，限制和权限 fail closed，缺文件使用已记录默认。
3. 同一真实 Git 仓库的两个 Task 拥有不同 branch/worktree，并在 worktree ready 后实际重叠执行。
4. 同仓库 Git 控制 side effect严格串行，lease 不覆盖正常角色循环。
5. global/per-repository permits 在成功、冲突、取消、panic、degraded 和 shutdown 下不泄漏或超发。
6. Scheduler 公平性、五种服务端 queue reason、无队列位置/ETA 和跨仓库跳过均符合本规格。
7. queue cap 在 SQLite 事务内不超卖，create/retry 幂等重放不被 429 遮蔽。
8. data/Git/runtime 三类 managed scope 的准入、去重、滞回、unavailable 和 critical stop 可确定重现。
9. critical disk 与用户取消的首次 intent、review finalization 和 late runner result 只产生规定的唯一终态。
10. 任何已启动进程树未确认退出前不释放 active permit或恢复调度。
11. worktree reservation 的 ready/inconsistent/abandoned/unavailable 路径不猜测、不覆盖、不自动清理。
12. v1/v2/v3 数据库无伪造数据地升级 v4；历史 Task/event/artifact/review/readiness 语义不漂移。
13. 冷启动只中断 Running 并重新扫描 Queued；优雅关闭/degraded 先停止并确认全部 active 进程树，再完成 durable intents，最后中断其余未完成 Task。
14. TaskStatus 六态、Project 3 quality gate 和 11 种持久 task event schema 保持。
15. Bootstrap、分段 SSE、OpenAPI、generated TypeScript 和 React 对 instance identity、scheduler exact 因果水位、stale 和停止分类一致。
16. P4-A 不提供 merge、worktree cleanup、artifact/history lifecycle、dynamic settings、OS sandbox 或发行能力。
17. 新鲜验证至少通过：

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
npm --prefix web run e2e
Remove-Item Env:CODING_AGENT_E2E_BINARY
cargo build --release --locked --offline -p coding-agent-app --features embedded-web
node scripts/check-placeholders.mjs
git diff --check
```

macOS/Linux 执行相同门禁时，把三行 PowerShell E2E 环境变量操作替换为：

```bash
CODING_AGENT_E2E_BINARY="$PWD/target/debug/coding-agent-app" npm --prefix web run e2e
```

18. Windows、macOS、Linux 的相关 process/Git/storage 测试通过；平台不能实现所需安全语义时 fail closed。
19. 独立代码审查确认 Blocker/High 为零，并人工核对最终 diff、迁移和新鲜测试证据。

## 22. 实施顺序约束

后续实现计划必须按依赖拆成可单独验证的 TDD 任务：

1. RuntimeConfig exact schema、bounds、私有文件加载和 Cargo jobs 纯计算。
2. SQLite v4 stop-intent/queued index、queue-cap create/retry 和分场景 recovery。
3. 纯 Scheduler 状态机、durable Git key 到对象 identity 的 grouping、fairness、reason 和 permit ledger。
4. RepositoryControlCoordinator、RunContext lease 和 worktree reservation/reconciliation。
5. StorageMonitor port、平台 volume sampler、dedup、admission、hysteresis 和 critical 通知。
6. TaskManager/StoreWriter claim、stop-intent、quality-finalization、shutdown/degraded 和 process cleanup 集成。
7. Scheduler projection精确因果水位、API/OpenAPI/SSE manifest/chunk contract。
8. React scheduler state、stale、queue reason、capacity、queue-full 和 stopping UI。
9. offline concurrent E2E、崩溃/故障注入、跨平台/压力测试、文档和全量验证。

每项先写失败测试，再做最小实现并运行相关测试。跨层 schema 变更必须在同一任务中更新 Rust DTO、OpenAPI、生成 TypeScript、runtime validator、reducer 和 fixtures。实现期间不得提前开始 P4-B merge/cleanup、P4-C history/artifact lifecycle 或 P4-D packaging/live-provider 行为。
