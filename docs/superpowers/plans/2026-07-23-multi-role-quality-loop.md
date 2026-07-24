# Project 3：多角色质量闭环实施计划

> 状态：已通过 Runner、Store、API/UI 三路独立实施复核
> 执行规则：手工按任务顺序实施 TDD；执行期间不得调用或使用 `superpowers` 技能。

**目标：** 在 Project 2 的单 worktree、单 provider task session 和全局并发 1 基础上，实现一次 Planner、最多三轮 Executor/Reviewer 的确定性质量闭环。只有最终 workspace generation 的全部必需检查通过、Reviewer 完整读取同一 digest 的 diff 并批准、终态复核仍一致时，Task 才能原子进入 `Completed + ReviewApproved`。

**源规格：** `docs/superpowers/specs/2026-07-23-multi-role-quality-loop-design.md`，已于 2026-07-23 获用户书面批准。规格与本计划冲突时以规格为准，停止实现并修订计划，不能现场发明新行为。

**架构：** 依赖方向锁定为 `app -> {api,store,core,provider,runtime}`、`{provider,runtime} -> core -> domain`，Store/API 也只依赖 domain 中立类型；禁止任何下层反向依赖 app。Project 3 在任务 5 显式为 core 增加 domain 依赖，不复制第二套同名 quality DTO。领域 crate 保存持久/wire 中立类型；Store 负责 v3 schema、不可变 review evidence 与最终原子事务；core 在一次 attempt 内持有共享预算、workspace checkpoint、检查 ledger 和角色编排；app 继续是唯一组合根、Task terminal owner 和 StoreWriter 使用者；API/UI 只投影服务端 readiness 与证据。

**技术栈：** Rust 1.97、edition 2024、Tokio、Serde、SQLx/SQLite、Axum/OpenAPI、React/TypeScript/Vite/Vitest/Playwright。全部默认验证离线，不联系真实 provider。

## 全局执行约束

- 不调用 `superpowers` 技能；本文件位于既有文档目录仅为沿用仓库组织方式。
- 每个任务严格执行 red -> green -> refactor：先添加聚焦失败测试并确认失败原因，再做最小实现，再运行聚焦与受影响回归。
- 不覆盖或整理用户的无关改动；每项完成后检查 `git status`、聚焦 diff 和 `git diff --check`。
- 不创建真实 provider Task、不发送真实 provider 请求；任何 live smoke 都需要用户另行明确授权并限定次数。
- 不改变 Project 2 的单 worktree、单 task session、全局并发 1、当前用户权限与非 OS 沙箱边界。
- Planner 只运行一次；返工只发生在 Executor/Reviewer 之间；最多两次返工、三次审查。
- 三个角色每轮使用全新 transcript；opaque reasoning 只在同一 role run 的 tool-call 往返中延续。
- 模型不能直接声明检查通过、workspace digest、diff coverage、delivery readiness 或 Task 终态；这些值都由 core/runtime/store 产生。
- 所有检查身份、工具权限、required action、数量/大小和阶段预算在 provider schema 与 core wrapper 两层 fail closed。
- SQLite 生产写入继续只通过 StoreWriter；Runner 不直接写 Store，TaskManager 仍是唯一 terminal lifecycle owner。
- 新 Task 的通用 `Running -> Completed` 写入口必须被封死；历史 `Completed + Unreviewed` 只读兼容。
- 每次提交 review/final outcome 前先持久化并确认当前 generation 的 diff/test panel barrier。
- Store、API、SSE 和 UI 不保存或展示 transcript、reasoning、raw provider response、秘密、绝对路径或未脱敏命令输出。
- 默认测试使用 scripted provider、临时 SQLite 和临时真实 Git/Cargo worktree，必须确定性且可重复。
- 不在 Project 3 中实现 merge、worktree cleanup、并发提升、任意 shell、人工 override 或 Project 4 行为。

## 锁定的归属映射

```text
crates/coding-agent-domain/
  src/{quality,event,task,lib}.rs
  tests/{state_machine,quality_loop}.rs
crates/coding-agent-store/
  migrations/0003_multi_role_quality.sql
  src/{reviews,tasks,projection,migrate,lib}.rs
  tests/{migrations,reviews,tasks,projection}.rs
crates/coding-agent-core/
  src/{quality,budget,role,role_loop,multi_role,model,ports,event,error,lib}.rs
  tests/{quality_state,budgets,role_contracts,role_loop,multi_role_orchestrator}.rs
crates/coding-agent-runtime/
  src/{cargo_tools,diff,fingerprint,runtime_session,role_runtime,lib}.rs
  tests/{cargo_tools,diff,quality_runtime,role_runtime}.rs
crates/coding-agent-provider/
  src/{protocol,client,error,lib}.rs
  tests/{schema,protocol,contract,role_contract}.rs
crates/coding-agent-app/
  src/{coding_agent_runner,store_writer,task_manager,event_dispatcher,lib}.rs
  tests/{coding_agent_runner,store_writer,task_manager,degraded_recovery,offline_e2e,multi_role_offline_e2e}.rs
crates/coding-agent-api/
  src/{contract,router,sse,backend}.rs
  tests/{openapi,router,sse}.rs
web/
  openapi.json
  src/api/{types,sse,client}.ts and generated/schema.d.ts
  src/state/{model,reducer,useAgentState}.ts
  src/components/{Sidebar,PlanPane,ActivityPane,ReviewPane,ResultPane,TaskWorkspace}.tsx
  src/styles.css and adjacent tests
  e2e/{lifecycle,ui-edge-cases,local-app}.spec.ts
README.md
```

采用更小模块时可以调整新文件名，但不能改变归属、依赖方向或把持久状态放回 app/core 的临时字符串。

## 任务 1：建立质量领域类型与终态矩阵

- [x] 在 `coding-agent-domain/tests/quality_loop.rs` 先添加失败测试，覆盖 `DeliveryReadiness` 三态、WorkspaceDigest/RequiredCheck/CheckEvidence/ReviewFinding/ReviewEvidence 的边界、canonical selector、`integration_test -> package`、system decision 只能 changes_requested，以及 Approved/Rejected 与 TaskStatus/failure 的组合矩阵。
- [x] 扩展 `state_machine.rs`，证明旧 `Completed + Unreviewed` 可读取，新 Approved 只能配 Completed、Rejected 只能配 `Failed + REVIEW_REJECTED`，Unreviewed 可配全部合法 lifecycle 状态。
- [x] 新增 `src/quality.rs` 并从 `lib.rs` 导出有验证构造器的领域类型；generation 上限固定为 `Number.MAX_SAFE_INTEGER`，digest 固定 lowercase 64 hex，数组/字符串/JSON bounds 与规格一致；持久写请求使用不含 Store 时间/event ID 的 `NewReviewEvidence`。
- [x] 为 `Task.delivery_readiness` 提供 legacy serde default `Unreviewed`，并在 `Task::try_from_stored` 执行 lifecycle/readiness/failure 交叉校验。不删除 `TaskStatus` 六态，避免前端或 Store 自行猜测组合。
- [x] 验证：

```powershell
cargo test -p coding-agent-domain --test quality_loop
cargo test -p coding-agent-domain --test state_machine
cargo fmt --all --check
```

检查点：领域类型不能依赖 Store、core、app 或 API DTO。

## 任务 2：扩展计划与活动的 legacy-compatible 领域模型

- [x] 先扩展 domain/event 的 serde unit tests，确认旧 plan/activity/event JSON 仍可解码为 `format_version=0`、空结构化字段和 `System/role_run=null`；此任务不依赖尚未建立的 v3 Store projection。
- [x] 为 Project 3 plan 增加 `format_version=1`、summary、description、acceptance criteria、initial checks；Activity 增加 actor/role_run。Task readiness 已在任务 1 完成，TaskDetail reviews 留到任务 4。
- [x] 此任务不增加 `ReviewUpdated` enum variant 或 `TaskDetail.reviews`，避免在 v3 evidence loader 尚未建立前打破 Store 的穷尽匹配；两者在任务 4 与全部 Store 消费端原子落地。
- [x] 添加 plan/activity exact JSON、unknown/invalid nested data 与 legacy default 边界测试。
- [x] 验证：

```powershell
cargo test -p coding-agent-domain
```

## 任务 3：增加 SQLite v3 schema 与统一 Task readiness mapper

- [x] 在 `coding-agent-store/tests/migrations.rs` 先添加失败的空库 v3、真实 v1->v3、v2->v3、重复 migrate、失败回滚和 `PRAGMA foreign_key_check` fixtures。
- [x] 新增 `0003_multi_role_quality.sql`：`task_review_evidence`、`task_delivery_state`、event parent tuple UNIQUE index、复合/判决感知 FK、STRICT/TEXT JSON/范围/CHECK、`{"evidence_ref":true}` marker 契约及 evidence/delivery UPDATE/DELETE abort triggers。
- [x] 测试非法 `system+approved`、SQL NULL coverage、非法 JSON/json_type、错误 round/generation/digest、event task/kind 不匹配、删除/修改 evidence 和 delivery 均被 DDL 拒绝；canonical typed 重编码属于任务 4 的 row codec，不要求 migration 单独证明。
- [x] 修改 `tasks.rs`/`projection.rs`，让 bootstrap/list/detail/create/retry/cancel/lifecycle 全部复用 `tasks LEFT JOIN task_delivery_state` typed mapper；缺行映射 Unreviewed，非法终态组合 fail closed。此阶段补上旧 plan/activity/lifecycle Store projection fixtures。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test migrations -- --nocapture
cargo test -p coding-agent-store --test tasks
cargo test -p coding-agent-store --test projection
```

检查点：迁移不回填旧 Completed 的批准状态。

## 任务 4：实现不可变 review 与最终原子事务

- [x] 在同一 red/green 变更中给 domain 增加 `review.updated` 第 11 种 event kind/payload、保持 envelope `schema_version=1`，并更新 Store 的全部穷尽匹配、event loader 与 `TaskDetail.reviews`；测试 128 KiB evidence、192 KiB wire event 和 legacy replay，不能留下中间不可编译提交。
- [x] 新增 `coding-agent-store/tests/reviews.rs` 的失败测试，覆盖 round 1/2 非终态 review、round 3 rejected、任意 round approved、event/evidence/last_event_id/delivery/Task/lifecycle 的原子顺序。
- [x] 先写明确绕行红灯：`Store::transition_with_event(...Completed)` 不能完成 Task且不能新增事件；只有 `finalize_reviewed_task` 可以产生新 Completed。任务 13 再对 StoreWriter 公共入口重复同一断言。
- [x] 在每个 SQL 步骤注入失败，证明没有 partial evidence、孤儿 marker event、无 evidence readiness 或无 readiness terminal Task。
- [x] 新增 `src/reviews.rs` 的 `record_review` 与 `finalize_reviewed_task` typed 操作；严格 Existing-first，同 canonical input 返回原 event ID，不同 input 或 partial tuple 返回 invariant conflict。
- [x] 从通用 `TaskTransition`/`transition_with_event` 生产路径删除或拒绝 `Running -> Completed`；只有 final approved transaction 能产生新 Completed。
- [x] event loader 按 `(event_id, task_id, event_kind)` join typed evidence，round 从 evidence row 取得；marker 不作为第二份证据解析。row codec 必须 canonical decode/re-encode，非法或非 canonical typed JSON fail closed。
- [x] retry 创建全新 Task，显式断言 `reviews=[] + Unreviewed`，不能复制旧 delivery/evidence；`recover_incomplete` 只处理中断的 Queued/Running，不能改写已提交 final tuple。
- [x] Store 层只测试冷启动所需的 `recover_incomplete` 数据语义：中间 reviews 保留，Interrupted/Failed/Cancelled 始终 Unreviewed。进程内 pending-first 编排留到任务 13 的 TaskManager/StoreWriter 测试。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test reviews -- --nocapture
cargo test -p coding-agent-store --test lifecycle
cargo test -p coding-agent-store --test projection
```

## 任务 5：实现 Task 级预算、checkpoint 和 required-check ledger

- [x] 先修改 `coding-agent-core/Cargo.toml`，只增加对 `coding-agent-domain` 的向下依赖；core 直接复用 Task 1 的 RequiredCheck/CheckEvidence/ReviewEvidence/NewReviewEvidence，禁止复制另一套结构。
- [x] 在 `coding-agent-core/tests/budgets.rs` 先添加失败测试，覆盖 Task 60 responses/96 calls/8 MiB provider、768 KiB retained results、角色 ceilings/leases、checked arithmetic、阶段预留和新 role run 不重置。
- [x] 精确测试 Executor 在启动前为下一 Reviewer 保留 6 responses、6 calls、184 KiB；Reviewer coverage 强制路径最多消耗 6 responses/calls，retained wrapper 只计一次，transcript 重编码只进入 provider ledger。
- [x] 在 `quality_state.rs` 测试 generation 0、same fingerprint、A->B->A、外部修改、MAX_SAFE_INTEGER overflow、digest 格式以及 generation 变化清空 current observations。
- [x] 测试同 CheckId 重跑先撤销旧 passed；latest failed/cancelled 替换旧值；required checks 只增不减、最多 16、至少 cargo_test；approved 必须每项 latest current passed。
- [x] 新增 `TaskBudgetLedger`、`WorkspaceCheckpoint`、`RequiredCheckLedger` 和唯一的 TestSnapshot projector；Planner/Executor/Reviewer ceilings 分别固定为 8/12/128 KiB、20/32/256 KiB、10/16/256 KiB，queued/running/terminal case 字段、名称与顺序严格按规格。
- [x] 验证：

```powershell
cargo test -p coding-agent-core --test budgets --locked --offline
cargo test -p coding-agent-core --test quality_state --locked --offline
```

## 任务 6：提供类型化检查目录与 ValidationRuntime

- [x] 先在 runtime 测试中证明 core 不从 repository-context 或 ToolResult 文本反解析 package/test/status/duration。
- [x] 扩展 core ports：`RepositoryCheckCatalog` 返回可信 metadata selectors；`ValidationRuntime` 接受 canonical RequiredCheck 并返回 `ValidationObservation` typed 字段。
- [x] 在 `cargo_tools.rs`/`runtime_session.rs` 复用 Project 2 的受控 Cargo adapter，固定 timeout 产品配置；不存在任意 argv/cwd/manifest/target/config 注入。
- [x] 测试 check/test success、nonzero、timeout、cancel、truncated model result、检查前后 workspace 变化和 `integration_test` 必须绑定 package。
- [x] 验证：

```powershell
cargo test -p coding-agent-core --test ports --locked --offline
cargo test -p coding-agent-runtime --test cargo_tools --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test quality_runtime --locked --offline
```

## 任务 7：实现 Reviewer 专用完整 diff coverage

- [x] 在 runtime/core 中先添加 manifest/chunk 失败测试：稳定排序、domain separator SHA-256、0..=8 chunks、每批连续最多 2 chunks、最多 4 批、binary/非 UTF-8/typed payload 超限/截断 fail closed。
- [x] 实现 `review_diff_manifest` 与 `review_diff_chunks` typed port；同一稳定表示按 generation+digest 缓存，采集期间 checkpoint 改变则整次失效。runtime 只约束 typed payload/chunk bounds，不在 provider/core wrapper 尚未存在时宣称证明 wrapper-inclusive 大小。
- [x] 把 manifest retained result <=24 KiB、单 chunk <=20 KiB、batch <=40 KiB 和总 reservation <=184 KiB 的 wrapper-inclusive encoder 测试明确留到任务 9；任务 11 再验证该编码结果满足 approval coverage gate。
- [x] 生成并验证 `ReviewCoverageEvidence`；approved 要求完整覆盖 `0..total_chunks`，terminal manifest SHA/generation/digest 必须重算一致；changes_requested 可在 blocking finding 后提前结束。
- [x] 保留普通 Git/read 工具用于辅助调查，但测试证明它们不能满足 coverage gate。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test diff --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test quality_runtime --locked --offline
cargo test -p coding-agent-core --test quality_state --locked --offline
```

## 任务 8：扩展 action/control 契约、provider schema 与精确 RequiredAction

- [x] 先新建纯契约测试 `coding-agent-core/tests/role_contracts.rs`；定义四个 strict terminal controls `submit_plan|submit_execution|submit_review|report_blocked` 和一个非终止 control `update_plan_progress`，覆盖 unknown field、nested bounds、canonicalization、secret/redaction mutation、solo-batch 约束和 terminal/runtime 混批零执行。`ActionRequest::{Runtime,Control}` 在类型层隔离，controls 不能伪装成 ToolRuntime 请求。
- [x] 把 `ModelToolChoice::RequiredCargoTest` 泛化为 typed `RequiredAction`；validation 绑定 CheckId/kind/package/integration-test，coverage 绑定 generation/digest/start/count，terminal 绑定预期 control kind。
- [x] 让 provider 从每次 `ModelRequest.allowed_actions` 按角色只编码允许工具和 exact const/single-value enum；不能继续向所有角色暴露 Project 2 的八个工具。保持 strict、required_as_required、required_as_auto 三种已有 wire 模式及 core exact-one 门禁。
- [x] 测试同名工具不同 selector、错误角色工具、多个/零个 required call、普通 final text、空批次和重复 tool_call_id 全部 fail closed；`update_plan_progress` schema/解码和匹配 ToolResult 在 provider executor contract 中单独覆盖。
- [x] thinking/reasoning 只在同 role transcript 往返；角色切换时 provider request 不含上一角色 assistant/tool/reasoning/metadata。一个 `start_task()` session/clone 跨全部角色共享 8 MiB provider ledger，不能每轮新建 client budget。
- [x] 验证：

```powershell
cargo test -p coding-agent-core --test role_contracts --locked --offline
cargo test -p coding-agent-provider --test role_contract --locked --offline -- --nocapture
cargo test -p coding-agent-provider --test protocol --locked --offline
cargo test -p coding-agent-provider --test contract --locked --offline -- --nocapture
cargo test -p coding-agent-core -p coding-agent-provider --all-targets --locked --offline
cargo check -p coding-agent-app --all-targets --all-features --locked --offline
```

检查点：公共 model/action API 变化后，旧 AgentLoop 与 app 在任务 13 切换生产入口前仍必须编译。

## 任务 9：建立角色作用域循环与 Planner

- [x] 先用 scripted provider/runtime 测试 RoleKind/role_run/tool ID namespace、全新 transcript、权限矩阵、activity actor 标签和 shared ledger lease；同 opaque tool ID 可跨 role run 复用但同 run 不可重复。
- [x] 从现有 `AgentLoop` 提取可复用、无 Project 2 单角色终态假设的 role loop；保留批次原子预检、顺序执行、取消、秘密和 provider 字节计数。
- [x] 新建独立 RoleTranscript：每轮只从新的 `[system,user]` 开始，handoff 只含规范化领域对象且总量 <=256 KiB；上一角色 calls/results/final/reasoning/request metadata 均不可见。
- [x] 新增围绕同一 `Arc<RuntimeSession>` 的 role-scoped runtime adapter，独立于 core policy 再次限制 Planner 只读、Reviewer 无 replace、Executor 无 reviewer-diff；control action 在类型上永远不能进入 runtime。
- [x] 新增唯一 canonical retained-result encoder，并在 wrapper-complete bytes 上证明 review manifest <=24 KiB、单 chunk <=20 KiB、双 chunk batch <=40 KiB、完整 coverage <=184 KiB；这些结果与 TaskBudgetLedger 使用同一计量口径。
- [x] 扩展 core event port，提供显式 `flush_checkpoint(generation)`/等价 durable barrier；普通 `emit(Diff|Tests)` 进入 debounce queue 不能冒充已持久化 ack。Planner progress/result 与 Executor 交审都必须能够等待该 port。
- [x] 实现 Planner policy：只允许 list/read/search、一次 `submit_plan|report_blocked`，禁止 replace/Cargo/Git/diff/review tools。
- [x] 验证 PlanSubmission bounds、1..=32 steps、验收条件、1..=16 initial checks、至少 cargo_test；core 生成 step/check IDs，等待 plan event durable ack 后才进入 Executor。
- [x] 验证：

```powershell
cargo test -p coding-agent-core --test role_loop planner --locked --offline -- --nocapture
cargo test -p coding-agent-core --test role_loop --locked --offline
cargo test -p coding-agent-provider --test role_contract planner --locked --offline
cargo test -p coding-agent-runtime --test role_runtime --locked --offline
```

## 任务 10：实现 Executor 轮次

- [x] 先测试 Executor 1..3 的允许工具、`update_plan_progress` 只有在 durable event ack 后才返回匹配 ToolResult、不能追加 Planner steps，以及正确的 rework banner；task-global activity ID 的 app 映射留到任务 13。
- [x] 实现缺失 required checks 的 typed required actions、latest observation 替换、聚合 TestSnapshot、workspace generation/digest 更新；交给 Reviewer 前必须调用任务 9 的显式 durable barrier，不能只把 diff/test 留在 debounce queue。
- [x] `submit_execution` 只接受有界摘要；core 必须自行验证全部 current checks passed、checkpoint 稳定和下个 Reviewer reservation。
- [x] `report_blocked` 直接形成阶段化 `Failed + Unreviewed`；普通 final text、无 evidence、越权或预算不足使用固定 failure code。
- [x] 验证：

```powershell
cargo test -p coding-agent-core --test role_loop executor --locked --offline -- --nocapture
cargo test -p coding-agent-core --test role_loop --locked --offline
cargo test -p coding-agent-core --test quality_state --locked --offline
cargo test -p coding-agent-provider --test role_contract executor --locked --offline
```

## 任务 11：实现 Reviewer 轮次与 system invalidation

- [x] 先测试 Reviewer 1..3 的允许工具、fresh transcript、完整 diff coverage、可选新增检查和 `approved|changes_requested` findings 关系。
- [x] coverage tracker 必须以“模型已看见”为准：chunk ToolResult 仅生成但尚未进入下一 provider request 时不算 covered；携带最后 chunk result 的 terminal-control 请求发出后才可覆盖；该请求发送失败时不得产生 approved evidence。
- [x] Reviewer 禁止 replace/plan update；不自动重跑 Executor 的 current passed checks；新增检查在工具执行前进入 append-only ledger，提交新增未运行检查只允许 changes_requested。
- [x] approved 验证全部 latest passed、完整 coverage、无 blocking finding；changes_requested 至少一条 blocking。Reviewer role 只返回 typed decision；第三轮 rejection 到 `REVIEW_REJECTED` 的 Task outcome 由任务 12 orchestrator 断言。
- [x] 测试 Reviewer Cargo/Git 导致 workspace 变化时 core 中止该 role，以 `decision_source=system` 写固定 blocking finding，计入 round 并进入返工/最终拒绝；绝不冒充 Reviewer verdict。
- [x] 验证终态复采 fingerprint/diff/manifest/evidence 任一不一致均不能批准。
- [x] 验证：

```powershell
cargo test -p coding-agent-core --test role_loop reviewer --locked --offline -- --nocapture
cargo test -p coding-agent-core --test role_loop --locked --offline
cargo test -p coding-agent-core --test quality_state --locked --offline
cargo test -p coding-agent-provider --test role_contract reviewer --locked --offline
```

## 任务 12：实现 MultiRoleOrchestrator 状态机

- [x] 在 `multi_role_orchestrator.rs` 先覆盖首轮批准、一次返工批准、两次返工批准、第三轮拒绝的完整调用序列。
- [x] 在 orchestrator 层明确断言只有第三轮有效 `changes_requested` 才返回 Rejected/`REVIEW_REJECTED`；前两轮只进入 Executor 返工，role 自身不结束 Task。
- [x] 覆盖每阶段 blocked、invalid output、provider/runtime/timeout/secret/context/step/task budget、panel ack failure、coverage limit、generation overflow 和 evidence mismatch 的固定 `Failed + Unreviewed` 矩阵。
- [x] 证明 Planner 恰好一次，角色历史互不可见，单 worktree/session/checkpoint/budget/cancel domain 不重置，最多七个 role runs。
- [x] TaskManager cancellation token 在模型、工具、fingerprint、event barrier 与 finalization 前均生效；orchestrator 只返回 typed Approved/Rejected/Failed/Cancelled outcome，不直接改变 Task。
- [x] 验证：

```powershell
cargo test -p coding-agent-core --test multi_role_orchestrator --locked --offline -- --nocapture
cargo test -p coding-agent-core --all-targets --locked --offline
```

## 任务 13：接入 CodingAgentRunner、StoreWriter 与 TaskManager

- [x] 先扩展 app 测试，证明 `CodingAgentRunner` 在一个 attempt worktree/provider task session 内驱动 orchestrator；把无负载 `RunnerOutcome::Succeeded` 替换为携带 `NewReviewEvidence` 的 Approved/Rejected，生产路径不能再丢 completion evidence。
- [x] 扩展 RunnerEvent/投影：structured plan、actor/role_run activity、workspace-generation diff/test 和 review barrier；app adapter 实现任务 9 的 `flush_checkpoint(generation)`，先 flush EventProjection debounce，再等待 Store 返回 durable event ID。所有七个可能 role runs 使用 task-global activity ID namespace，不能在角色切换时重置碰撞。
- [x] 为 StoreWriter 增加 `RecordReview`/`FinalizeReviewedTask` 操作、故障注入 kind、Existing replay wake 与最后 event ID flush；Runner 仍不能取得 Store，不能用 `TransitionWithEvent` 模拟质量事务。
- [x] 增加 StoreWriter 绕行红灯：生产 handle 不暴露 Completed transition，底层若收到该路径也必须失败、不新增 event 且不 wake；只有 `FinalizeReviewedTask` 能完成 Task。
- [x] 修改 TaskManager mailbox，并在 ActiveRunner 记录独立 `user_cancel_requested`：cancel 先处理时即使 runner 忽略 token 后返回 Approved，也必须转 Cancelled；Approved 先进入不可中断 final Store transaction 后 cancel 返回不可取消。
- [x] `PendingDurableResult` 只保存可用 Existing-first 精确比较的 typed `RecordReview`/`FinalizeReviewedTask` 稳定请求；普通 plan/activity/diff/test panel append 没有幂等键，不得盲目 replay 或冒充该类型。in-process degraded recovery 必须先按原序 replay quality pending，再 recover 其余 Running/Queued、flush 最大 event ID、切回 Ready；typed conflict 保持 frozen，不能沿用当前先 interrupt 再丢 pending 的顺序。
- [x] cold start 无内存 pending 时仍把未完成 Running 标为 Interrupted，保留 reviews 且无 readiness；不能从 worktree/activity 推断批准。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test coding_agent_runner --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test store_writer --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test task_manager --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test degraded_recovery --features test-support --locked --offline -- --nocapture
```

## 任务 14：原子扩展 API、OpenAPI 与真实事件投影

- [x] 先更新 `coding-agent-api/tests/openapi.rs`，锁定 required `delivery_readiness`、TaskDetail.reviews、plan/activity 新字段、完整 Review/Check/Coverage DTO、nullable/array/enum/bounds 和 `review.updated` discriminator/oneOf。
- [x] 更新 `contract.rs`、手写 event mapping、OpenAPI export、`web/openapi.json` 和生成 TypeScript；同一原子变更还要更新 `web/src/api/types.ts`、SSE allowlist/最小 decoder、state model/reducer 的 typed `review.updated` append 分支及全部受新增 required 字段影响的 fixtures。`TaskEventKindDto` 精确为 11 种，禁止用 `Record<string, unknown>` 逃避 typed payload。
- [x] 在 API/app 测试中覆盖 bootstrap/list/create/get/retry/cancel 的统一 readiness、reviews round 排序、REST/SSE 相同 DTO，以及 diff/test -> review.updated -> lifecycle 顺序。
- [x] `review.updated` 增量事件只追加 review，不在 API 层推导 Task status/readiness；真实 evidence 来自 Store typed join，不从 marker 重建。
- [x] 验证：

```powershell
cargo test -p coding-agent-api --test openapi --locked --offline
cargo test -p coding-agent-api --test router --locked --offline
cargo test -p coding-agent-api --test sse --locked --offline
cargo test -p coding-agent-app --test server --locked --offline
cargo test -p coding-agent-app --test event_dispatcher --locked --offline
cargo run --locked --offline -p coding-agent-api --bin export_openapi -- web/openapi.json
npm --prefix web run api:generate
npm --prefix web run api:check
npm --prefix web run test:run -- src/api/sse.test.ts src/state/reducer.test.ts
npm --prefix web run typecheck
```

## 任务 15：建立 Web 验证、review reducer 与 cursor recovery

- [x] 新建共享 `web/src/api/validation.ts` 及测试；REST TaskDetail 验证完整跨轮历史，SSE 单事件只验证自包含 shape/bounds/唯一引用/generation/digest/approved/coverage 关系。
- [x] 测试拒绝额外/缺失字段、错误 null、非安全整数/摘要、非法 selector、重复 ID、错误 added delta、Plan v0/v1、Activity actor/role_run 组合和超 256 KiB frame。
- [x] 新建纯函数 review projection：同 `(task,round)` 同 canonical payload 幂等；完整历史下异 payload 触发 protocol conflict；缺历史只标 detail stale、推进 cursor 并请求有 event_cursor 边界的 TaskDetail refetch。
- [x] 在 `useAgentState` 测试 bootstrap high-watermark recovery：冲突 event 在 recovery 前不提交 cursor；快照替换后丢弃 `<= watermark` buffer，只应用更晚事件；refetch 失败保留 stale/retry。
- [x] review reducer 永远不自行改变 Task lifecycle/readiness；最终 Task 只由 lifecycle event 或新 REST/bootstrap snapshot 更新。
- [x] 验证：

```powershell
npm --prefix web run test:run -- src/api/validation.test.ts src/api/client.test.ts src/api/sse.test.ts
npm --prefix web run test:run -- src/state/reviewProjection.test.ts src/state/reducer.test.ts src/state/useAgentState.test.tsx
npm --prefix web run typecheck
```

## 任务 16：实现 readiness、Plan、Activity 与 ReviewPane

- [x] 先写组件测试，证明 Sidebar/TaskWorkspace 分开显示 lifecycle/readiness；历史 `Completed + Unreviewed` 显示免责声明，UI 不从 review 自行批准。
- [x] 扩展 PlanPane 显示 summary/description/acceptance/initial checks 和 v0 legacy 提示；ActivityPane 显示 `System`、`Planner #1`、`Executor #N`、`Reviewer #N`。
- [x] 新增 ReviewPane，面板顺序固定为 Failure -> Review -> Diff -> Tests -> Timeline；显示 round/source/verdict/findings/added+cumulative checks/coverage/evidence，并区分 system decision。
- [x] Diff/Test 标题明确使用 Workspace generation；正常返工后的旧 changes_requested 标 expected stale 而非 integrity warning，terminal Task 只比较 delivery 引用的 final review。
- [x] approved review 增量先到时显示等待终态事件；新 REST snapshot 已终态时立即采用。长 summary/finding/path/selector/digest 必须换行，无水平滚动。
- [x] 不显示 transcript/reasoning，不增加 merge、approve 或 override 控件。
- [x] 验证：

```powershell
npm --prefix web run test:run -- src/components/AppShell.test.tsx src/components/TaskWorkspace.test.tsx src/components/ReviewPane.test.tsx
npm --prefix web run typecheck
npm --prefix web run build
```

## 任务 17：离线多角色 E2E 与 legacy v2 用户旅程

- [x] 用 scripted fake provider + 临时真实 Git/Cargo worktree 新增 `multi_role_offline_e2e.rs`，覆盖首轮批准、一次返工、两次返工和最终拒绝；断言单 worktree/session、单 `start_task()` provider ledger、Planner 一次、fresh transcripts、typed observations/diff chunks、digest gate、SQLite/REST/SSE 一致。
- [x] 增加 cancel race、blocked、provider/runtime/store failure、budget exhaustion、Reviewer mutation、coverage limit、重启 Interrupted；网络 guard 证明不联系真实 provider。
- [x] 由 Rust test-support 在 `single_instance.rs` 调用 `Store::open`/migrate 前创建真实 v2 SQLite seed，不在 Node 中另写 SQLite；旧 Completed 经迁移后在 REST/SSE/React 中保持 Unreviewed 且没有伪 review。
- [x] `ProcessTestConfig` 保持 deny-unknown/全字段必填；新增 legacy seed/scenario 字段时同步更新 Rust、`localApp.ts` 与所有既有 scenario literals，不能让非本任务 E2E fixture 静默采用默认值。
- [x] Playwright 新增四条路径：首轮批准、一次返工后批准、最终拒绝、旧 Completed；断言 badge、ReviewPane、面板顺序、generation 文案和无 merge/approve 控件。
- [x] 更新 README 的多角色流程、readiness、当前用户权限/Cargo 风险、预算、恢复和“不自动 merge”边界。
- [x] 运行 Playwright 前确认项目既有 Chromium 或显式设置 `PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH`；任务 16 的 Web build 必须先生成被忽略的 `web/dist`。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test multi_role_offline_e2e --all-features --locked --offline -- --nocapture
cargo build --locked --offline -p coding-agent-app --features e2e
$env:CODING_AGENT_E2E_BINARY = (Resolve-Path '.\target\debug\coding-agent-app.exe').Path
npm --prefix web run e2e -- multi-role-quality.spec.ts
Remove-Item Env:CODING_AGENT_E2E_BINARY
```

## 任务 18：独立代码审查与完整验收

- [x] 独立审查重点覆盖 state machine、budget reservation、角色权限双门、diff coverage、SQLite transaction/FK/idempotence、cancel linearization、pending-first recovery、SSE cursor recovery 和 legacy compatibility。
- [x] 解决全部 blocker/high findings；每次修改先重跑受影响聚焦测试，之后使用最终代码运行完整验收。
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
npm --prefix web run e2e
Remove-Item Env:CODING_AGENT_E2E_BINARY
cargo build --release --locked --offline -p coding-agent-app --features embedded-web
node scripts/check-placeholders.mjs
git diff --check
```

## 完成定义

- [x] 18 个任务全部通过各自聚焦测试，且完整验收命令使用最终代码重新运行。
- [x] 三角色权限、预算、transcript、generation/digest/check/diff coverage 均有 fail-closed 回归。
- [x] v1/v2 数据、旧 plan/activity/event 和历史 Completed 在 Store/API/UI 中保持兼容且一律 Unreviewed。
- [x] 新 Completed 只能来自最终 approved 原子事务；第三轮有效 rejection 才能产生 ReviewRejected。
- [x] 最终 diff/test -> review.updated -> lifecycle 的持久顺序、SSE recovery 和 cancel/finalization race 均有确定性证据。
- [x] 不存在真实 provider 请求、秘密、raw reasoning、未授权 merge/cleanup/concurrency 或占位符。
- [x] 最终人工检查 `git status`、完整 diff、生成文件和测试记录后，才可声明 Project 3 完成。
