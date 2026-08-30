# Project 3：多角色质量闭环设计

> 日期：2026-07-23
> 状态：已于 2026-07-23 获用户书面批准；Project 3 已完成并验收
> 前置条件：Project 2 已完成并验收
> 2026-08-29 范围修订：本文旧称的后续 Project 4 能力按最终批准边界拆分为 P4-A 受控并发/资源准入、P4-B 显式本地交付/清理、未来 P4-C 历史与构件生命周期、未来 P4-D 发行/provider 加固；Project 4 本身仅为 P4-A + P4-B。此注释不改写 Project 3 的历史 TDD 事实。

## 1. 目标

Project 3 在 Project 2 的单 worktree 隔离执行之上增加一个确定性的三角色质量闭环：Planner 只读分析并提交结构化计划，Executor 按计划修改并验证，Reviewer 在全新上下文中检查最终差异和证据。Reviewer 不通过时，任务最多进入两轮 Executor 返工；只有最终 workspace generation 的全部必需检查通过、Reviewer 对同一 digest 明确批准且终态复核仍一致，任务才进入 `Completed + ReviewApproved`。

`TaskStatus` 继续只描述任务生命周期；新的持久 `delivery_readiness` 独立描述交付质量。Project 2 和更早的历史任务即使为 `Completed`，也一律保持 `Unreviewed`。Project 3 的批准是本产品中自动 Reviewer 对有界证据的批准，不代表人工审查、已合并、可部署、已签名或生产安全。

## 2. 范围

### 2.1 包含

- 在同一个任务 worktree 和 provider task session 内顺序运行 Planner、Executor、Reviewer。
- Planner 每个 Task 只运行一次；Reviewer 拒绝后仅在 Executor 与 Reviewer 之间返工。
- 最多两轮返工、三轮 Reviewer，且所有转换无歧义、有上限。
- 三个角色的工具权限、system policy、transcript 和 `tool_call_id` 命名空间隔离。
- 结构化 `submit_plan`、`submit_execution`、`submit_review`、`report_blocked` 控制动作。
- Planner 建立、Reviewer 只增不减的类型化必需检查集合。
- 任务级单调 workspace generation、完整 workspace digest 和 generation-bound 检查/审查证据。
- 独立持久的 `delivery_readiness`、逐轮不可变审查证据和 `review.updated` 事件。
- 最终审查证据、readiness、Task 终态和 lifecycle event 的单事务提交。
- REST、SSE、OpenAPI 和 React 工作台对计划、角色活动、审查历史及 readiness 的最小扩展。
- 旧数据库、旧 lifecycle JSON、旧 Completed Task 和现有 retry 语义的向后兼容。
- 离线、确定性的角色编排、迁移、恢复、原子性、API 和 UI 测试。

### 2.2 不包含

- 人工 Reviewer、交互式审批或运行中的“暂停等待用户”状态。
- 自动 merge、手动 merge、冲突处理、rebase、push、PR 创建或交付发布。
- 真实任务并发提升；生产全局并发仍为 1。
- 同仓库多任务协调、worktree 清理、历史清理、磁盘或进程资源配额。
- 新的任意 shell 工具、任意测试命令或仓库自定义脚本入口。
- OS 级文件系统、网络、CPU、内存或恶意代码沙箱。
- 多 provider 投票、不同模型强制分配、人工身份或密码学意义上的独立审计者。
- 安装器、签名、公证、自动更新及其他当时统称 Project 4 的发行能力；按 2026-08-29 范围修订归未来 P4-D。

### 2.3 信任与安全边界

Project 2 的威胁模型保持不变。角色权限是模型可见 schema 与 core/runtime capability 的双层协议限制，不是 OS 安全边界。Planner 和 Reviewer 没有第一方写文件工具；但是 Reviewer 允许运行 Cargo，Cargo/build/test 代码仍以当前 OS 用户权限执行，可能修改源码、访问 worktree 外路径或联网。每次检查前后都必须重新计算完整 workspace fingerprint，不能把“Reviewer 无写工具”表述为只读沙箱。

仓库文本、模型输出、provider metadata 和其他角色的摘要都不是控制面授权。所有控制 payload、路径、选择器、枚举、数量和大小在 core 中验证；角色 wrapper 对越权动作 fail closed；底层 runtime 仍执行 Project 2 的路径、命令和环境策略。秘密检测与脱敏应用于每个角色的输入、输出、工具参数、工具结果和角色间领域对象。任何结构化字段经脱敏会发生变化时，相关批次在产生副作用前以 `PROVIDER_SECRET_DETECTED` 失败。

## 3. 核心不变量

1. 一个 Task 只创建一个 attempt worktree、一个取消域、一个任务级 provider session 和一个共享预算账本。
2. Planner 只运行一次；正常路径固定为 `Planner -> Executor 1 -> Reviewer 1`，之后最多两次 `Executor -> Reviewer` 返工。
3. Task 在全部角色循环期间保持 `Running + Unreviewed`；中间 `changes_requested` 不是新的 TaskStatus 或 readiness。
4. Planner 和 Reviewer 不能调用 `replace_file`；Planner 不能运行 Cargo；Reviewer 只能使用批准的只读/Git/Cargo 工具。
5. 每个角色运行拥有全新 transcript。角色间只传递验证过且有界的领域对象，不传递 assistant/tool transcript、raw provider response、reasoning 或 metadata。
6. 单个角色内延续 Project 2 的批次语义：整批原子预检、按响应顺序串行执行、同序返回 tool result，不能共享或重排其他角色的批次。
7. workspace generation 在整个 Task attempt 内单调且 checked；不同角色或返工轮次绝不重置。
8. 每项检查与每轮审查都绑定同一 `{generation, digest_algorithm, workspace_digest}`。
9. 必需检查集合只能追加和去重，不能删除或放宽，且始终至少包含一个 `cargo_test`。
10. 旧 generation 的 passed 检查、Reviewer verdict 或 diff 不能支持当前 generation 的批准。
11. `ReviewApproved` 只允许与 `Completed` 共存；`ReviewRejected` 只允许与 `Failed` 共存；`Unreviewed` 允许所有生命周期状态，包括历史 Completed。
12. 最终 generation 的 diff 与聚合 test snapshot 必须先获得持久 ack，之后 final review evidence、delivery state、Task 终态和两个有序持久事件才可在同一 SQLite 事务中提交。
13. v3 新任务不存在通用的无审查 `Running -> Completed` 写入口；新的 Completed 只能由最终 approved 质量事务产生，历史 `Completed + Unreviewed` 仅可读取。
14. 第三轮的有效 review decision（Reviewer control 或明确标记为 system 的 workspace invalidation）为 `changes_requested` 时才能产生 `ReviewRejected`；角色错误、阻塞、取消、超时或预算耗尽绝不能伪装成审查拒绝。
15. 前端只展示服务端 readiness 和证据，不自行推导或授予批准。
16. Project 3 不打开 merge 或真实任务并发，也不删除任何 worktree 或审查历史。

## 4. 架构与依赖方向

现有 crate 依赖方向保持不变。Project 3 不创建三个顶层 `CodingAgentRunner`，而是在一次 Project 2 attempt 内增加 core 编排器：

```text
TaskManager
  -> CodingAgentRunner
      -> one worktree / cancellation domain / EventProjection
      -> one TaskModelSession / TaskBudgetLedger
      -> MultiRoleOrchestrator
          -> PlannerRoleRun (once)
          -> ExecutorRoleRun #1
          -> ReviewerRoleRun #1
          -> ExecutorRoleRun #2, if requested
          -> ReviewerRoleRun #2
          -> ExecutorRoleRun #3, if requested
          -> ReviewerRoleRun #3
```

- `coding-agent-core` 拥有角色、结构化产物、共享预算、checkpoint、必需检查和确定性状态机；不依赖 HTTP、SQLite、Git 路径或 UI。
- `coding-agent-provider` 按角色编码允许的 runtime/control action schema，并继续执行 OpenAI-compatible tool choice、大小限制和 reasoning round-trip。
- `coding-agent-runtime` 保持 Project 2 的八种底层工具与 fingerprint/snapshot 能力，并增加两个只读、Reviewer-only 的 typed diff coverage 操作；role-scoped wrapper 只暴露允许动作，不增加任意 Git/shell 接口。
- `coding-agent-domain` 定义 readiness、扩展计划、角色活动、审查证据和事件 payload。
- `coding-agent-store` 提供 v3 migration、不可变 review evidence、delivery state、投影和最终原子事务。
- `coding-agent-app` 仍负责 worktree provision、provider/runtime 组合、事件映射和 Task 终态所有权。
- `coding-agent-api` 和 `web` 只消费版本化领域 DTO，不复制质量门规则。

现有单角色 `AgentLoop` 的通用 transcript、预算、工具批次和 validation 逻辑应提取成可复用 role engine；Executor 可以复用其行为，但 workspace checkpoint、必需检查和任务级预算必须提升到 `MultiRoleOrchestrator`。不能通过创建多个旧 `AgentLoop` 而让 revision 或预算在每轮归零。

core 不能从给模型看的 repository-context 字符串或 opaque ToolResult 文本反解析权威检查证据。app/runtime 必须提供中立的 typed ports：`RepositoryCheckCatalog` 保存可信 metadata 发现的 package/integration-test selector；`ValidationRuntime` 接受规范化 RequiredCheck 并返回 `ValidationObservation { model_result, check, status, duration_ms, truncated }`。fingerprint before/after 仍由 checkpoint 逻辑绑定。模型看到的文本结果由同一 observation 投影，授权和 quality ledger 只读取 typed 字段。

## 5. 角色权限与运行产物

### 5.1 Planner

Planner 接收脱敏后的原始 Task prompt、只含允许 selector 的 repository context 和当前初始 checkpoint。它只允许：

- `list_files`
- `read_file`
- `search_text`
- `submit_plan`
- `report_blocked`

Planner 不允许 Git、Cargo 或写工具。它只运行一次，不能在 Reviewer 返工后重跑。正常结束必须以单独的 `submit_plan` 控制批次提交结构化计划；普通 final text、空调用批次或混合 runtime/control 批次均不是成功终态。

### 5.2 Executor

Executor #1 接收 Task、Planner 计划、当前 checkpoint 和必需检查；Executor #2/#3 另外只接收最新结构化 Reviewer findings 和累积检查集合。它允许 Project 2 的全部 runtime 工具：

- `list_files`
- `read_file`
- `search_text`
- `replace_file`
- `cargo_check`
- `cargo_test`
- `git_status`
- `git_diff`
- `update_plan_progress`
- `submit_execution`
- `report_blocked`

`update_plan_progress` 只能改变既有 step 的 status，不能修改、删除或追加 ID、标题、描述、验收条件或检查。单次调用可原子更新多个 step；状态只允许 `pending -> running|completed` 和 `running -> completed`，至多一个 step 为 running，不允许回退。返工时已完成的原始计划保持完成，补充工作通过带 Executor actor 的 activity 和 Reviewer findings 表达，不成为新的 PlanItem；UI 另以 Rework round banner 表示正在返工，避免把完成的原计划误解为整个 Task 已结束。

`update_plan_progress` 是非终止 control：core 必须先等待对应 PlanUpdated 的 durable ack，再向同一 `tool_call_id` 加入有界、core-generated ToolResult ack 并继续 provider loop。持久化失败时不发送成功 ack，当前角色按 event/store failure 结束。control call 仍遵守 transcript 中每个 tool call 都有同序结果的协议。

正常交审必须使用单独的 `submit_execution`。core 只在计划步骤全部完成、当前 generation 的全部必需检查已通过且 fingerprint 再次一致时接受。启动 Reviewer 前还必须 flush 当前 generation 的 diff/test panel events 并获得 durable ack。执行摘要是有界领域字段，不是批准证据。

### 5.3 Reviewer

每轮 Reviewer 接收全新的上下文：Task、Planner 计划、Executor 有界摘要、当前 checkpoint、累积必需检查、当前 generation 的检查摘要和先前 review evidence 的结构化摘要。Reviewer 不接收其他角色 transcript。它允许：

- `list_files`
- `read_file`
- `search_text`
- `cargo_check`
- `cargo_test`
- `git_status`
- `git_diff`
- `review_diff_manifest`
- `review_diff_chunks`
- `submit_review`
- `report_blocked`

Reviewer 不允许 `replace_file` 或计划更新。它可以运行额外检查，并在 `submit_review` 中把新检查加入累积集合。正常结束必须使用单独的 `submit_review`；裁决只允许 `approved` 或 `changes_requested`。

Reviewer 还拥有只对审查开放的 `review_diff_manifest` 与 `review_diff_chunks`。它们不是任意 Git 命令，而是 runtime 对当前稳定 checkpoint 生成的类型化、可覆盖证明的 diff 视图；第 8.4 节规定 approved 前的强制覆盖门。

Reviewer 不是安全沙箱或人类审查者。相同 provider/model 配置可供三个角色使用，但每轮消息历史必须独立。

## 6. 结构化控制对象

### 6.1 PlanSubmission

`submit_plan` payload 至少包含：

```text
PlanSubmission {
  summary,
  steps[{ title, description, acceptance_criteria[] }],
  initial_required_checks[]
}
```

约束：

- summary 最多 4,096 个 Unicode scalar；
- step 数量为 1..=32；
- title 非空且最多 256 scalar；
- description 最多 4,096 scalar；
- 每个 step 有 1..=8 条非空 acceptance criterion，每条最多 1,024 scalar；
- initial required check 去重后为 1..=16，且至少一项为 `cargo_test`；
- 整个 canonical JSON 编码不超过 64 KiB；
- step ID 和 check ID 由 core 按验证后顺序生成，provider 不能指定权威 ID。

计划中的检查只能是：

```text
CargoCheck { package? }
CargoTest  { package?, integration_test? }
```

selector 必须通过 Project 2 的 package/integration-test grammar 和 repository context 验证。timeout 不是检查身份的一部分，也不能由计划永久指定；运行时使用受控的产品配置。

### 6.2 ExecutionSubmission

`submit_execution` 只包含最多 4,096 scalar 的安全摘要。core 在接受前自行验证计划状态、必需检查、generation、digest 和稳定 terminal snapshot；模型不能在 payload 中声明 passed、批准或覆盖 checkpoint。

### 6.3 ReviewSubmission

```text
ReviewSubmission {
  verdict: approved | changes_requested,
  summary,
  findings[{ severity, message, path?, line? }],
  add_required_checks[]
}
```

约束：

- summary 最多 4,096 scalar；
- finding 数量为 0..=32，每条 message 非空且最多 2,048 scalar；
- severity 只允许 `blocking|advisory`；
- path 若存在，必须通过 Project 2 相对 worktree UTF-8 slash path 验证；line 若存在必须大于 0，且 line 存在时 path 必须同时存在；
- finding ID 由 core 生成，格式在一个 Task 内稳定且包含 review round 与 ordinal；
- 新增检查经过相同 selector 验证、规范化、去重，累积总数仍不得超过 16；
- 整个 canonical JSON 编码不超过 64 KiB。

`approved` 必须没有 blocking finding，且所有累积检查在当前 checkpoint 上有 passed evidence。`changes_requested` 必须至少有一个 blocking finding。advisory finding 可以与 approved 同时存在，不阻止交付。结构或语义不满足时属于无效 Reviewer 输出，不自动转换为另一种 verdict。

若 submission 追加了尚未通过工具调用加入的检查，core 先原子更新 ledger、发布当前 generation 的聚合 TestSnapshot 并等待 durable ack，之后才允许持久化 review evidence。这样 `review.updated` 不能先于它所声明的 required-check 集合；panel flush 失败结束为 Unreviewed，不提交该 review。

### 6.4 BlockedSubmission

任何角色可用 `report_blocked` 代替其正常终态。payload 包含一个受控 reason 和最多 4,096 scalar 的安全摘要。reason 只允许：

- `missing_required_context`
- `conflicting_user_requirements`
- `requires_goal_change`
- `unsupported_scope`

只有 `missing_required_context` 标记为 retryable；其余三类在相同 prompt 下不会自行消失，标记为 non-retryable。Task 不进入等待态，而是以当前角色和 reason 对应的稳定 failure code 结束为 `Failed + Unreviewed`。模型不能自行设置 failure code、message 或 retryable。

### 6.5 控制批次

终态控制动作 `submit_plan`、`submit_execution`、`submit_review`、`report_blocked` 必须是响应中唯一调用，不得与 runtime action、`update_plan_progress` 或另一个控制动作混合。`update_plan_progress` 本身也使用仅含一个控制调用的批次。任意混合、空批次、未知字段、重复字段、无效枚举、越权动作或大小超限都使整批零执行。

所有控制调用都计入角色和任务级 model-visible call 预算。它们由 core 解释，绝不能进入 `ToolRuntime::invoke`。

## 7. 编排状态机

### 7.1 正常路径

```text
Provisioned
  -> Planning
  -> Executing(round=1)
  -> Reviewing(round=1)
      -> approved -> FinalizingApproved
      -> changes_requested -> Executing(round=2)
  -> Reviewing(round=2)
      -> approved -> FinalizingApproved
      -> changes_requested -> Executing(round=3)
  -> Reviewing(round=3)
      -> approved -> FinalizingApproved
      -> changes_requested -> FinalizingRejected
```

最多两轮返工、三轮审查。review round 与紧邻的 Executor run 使用相同编号 1..=3；Planner 的 role run 固定为 1。Planner 不重新进入，review round 不复用 Task 的 `attempt` 或 retry 链语义。

### 7.2 TaskStatus 与 readiness 矩阵

| 结果 | TaskStatus | delivery_readiness |
|---|---|---|
| 最终 generation 审批通过 | Completed | ReviewApproved |
| 第三轮有效 changes_requested | Failed | ReviewRejected |
| Planner/Executor/Reviewer blocked | Failed | Unreviewed |
| 角色/provider/runtime/预算/证据错误 | Failed | Unreviewed |
| 用户取消 | Cancelled | Unreviewed |
| 启动或关闭恢复 | Interrupted | Unreviewed |
| Project 2 或更早的历史 Completed | Completed | Unreviewed |

Task 在 Planning、Executing、Reviewing 和两次返工之间始终为 `Running + Unreviewed`。中间 Reviewer evidence 通过 `reviews[]` 表达，不能新增 `Reviewing`、`Reworking` 或 `Blocked` TaskStatus。

### 7.3 角色终止与错误

- 普通 final text 不能结束任何角色；在当前 tool-choice 兼容模式返回 final text 时 fail closed。正常角色终态必须使用 typed control；第 8.5/8.6 节 checkpoint invalidation 是唯一由 core 中止 Reviewer 并形成 system decision 的例外。
- Planner 无有效 plan、Executor 无 current evidence、Reviewer verdict 不一致都属于相应阶段的 invalid-output failure。
- provider、runtime、timeout、secret、context 或预算错误保留当前阶段，结束为 `Failed + Unreviewed`。
- cancellation 在任意模型、工具、fingerprint、event 或 finalization 等待点生效，停止后续角色，执行 Project 2 的进程树清理和有界 terminal snapshot，结束为 `Cancelled + Unreviewed`。
- TaskManager actor mailbox 是 cancel 与 final outcome 的线性化点：若 cancel command 先被 actor 处理，token 已置位，随后即使收到 Approved outcome也必须转为 Cancelled，不能启动质量事务；若 Approved outcome 先被处理并进入不可中断的 final StoreWriter transaction，则随后 cancel 是 late cancel，看到 terminal Task 并返回不可取消。两种 mailbox 顺序都必须有确定性 race test。
- 一旦最终质量事务已提交，迟到 cancellation 或 runner outcome 不能逆转终态。

## 8. Workspace generation、digest 与检查证据

### 8.1 Task 级 checkpoint

编排器持有：

```text
WorkspaceCheckpoint {
  generation: u64,
  fingerprint: WorkspaceFingerprint,
  current_check_observations: map<CheckId, CheckEvidence>
}
```

worktree provision 后第一次稳定 fingerprint 建立 generation 0。随后每次成功写入、Cargo/Git 工具返回、Reviewer 命令结束、交审、审查提交和最终化前，都在需要时采集 Project 2 定义的完整 deliverable fingerprint。观测到与当前 fingerprint 不同的稳定值时，generation 使用 checked add 递增并清空 current check observations；同一 fingerprint 不递增。A->B->A 在每次稳定观测处产生两个新 generation，不能仅因 digest 再次相同复用旧证据。

Project 3 对新任务明确用“稳定 fingerprint 实际变化”取代 Project 2 的“每次成功 replace 都递增”内存语义；相同内容的成功 replace 不产生新 generation，旧 Project 2 事件不重写。generation 上限固定为 JavaScript `Number.MAX_SAFE_INTEGER`（9,007,199,254,740,991），因此同时可安全表示为 SQLite signed integer 和现有 wire number；下一次递增越界即 fail closed。现有 `EventProjection` 的 debounce generation 不是 workspace generation，必须删除歧义或重命名，不能持久化为质量证据。

### 8.2 Digest

workspace digest 直接使用 Project 2 对完整可交付集合计算的 32-byte `WorkspaceFingerprint`：tracked index 身份/状态/内容与全部非 ignored untracked deliverable，沿用 no-follow、稳定排序、前后 metadata/namespace 校验和大小上限。

持久格式固定为：

```text
algorithm = "workspace_fingerprint_v1"
value     = 64-character lowercase hexadecimal
```

不能对 UI patch、可能截断的 diff、Git 文本输出、文件时间戳或仅 tracked 文件求 digest。算法升级必须使用新 algorithm 值；不能在同名 v1 下改变输入集合或编码。

### 8.3 CheckEvidence

检查身份由规范化的检查 kind 与 selector 决定，忽略运行 timeout。重复的 Planner/Reviewer 检查折叠为同一个 core-assigned CheckId。每次运行记录：

- CheckId 与规范化 selector；
- 执行角色和 role run；
- generation、digest algorithm 和 digest；
- status、duration、经过脱敏和截断的摘要；单条摘要最多 2,048 个 UTF-8 字节；
- 结果是否被 runtime 截断。

接受某个 CheckId 的新检查调用时，core 先原子移除该 ID 的旧 terminal observation，再发布 queued/running 投影，因此旧 passed 不能在重跑期间继续满足门禁。检查结束后，若前后 fingerprint 稳定且仍为同一 generation/digest，则无论 passed、failed、cancelled 或 timeout 都以该次最新 attempt 替换 map 中同 ID 的旧 observation；timeout 在 wire status 中按 failed 投影。只有 command 成功、检查前后 fingerprint 相同、且之后再次观测仍为当前 checkpoint 的最新 passed observation 才能计为通过。workspace 变化会按第 8.1 节推进 generation 并清空整个 map；无法取得稳定 fingerprint 或 generation/digest 不一致时不留下 current observation，更不能恢复旧 passed。

每轮 ReviewEvidence.check_evidence 必须是提交瞬间累积 required-check 集合在 current map 中所有 terminal observations 的精确投影，每个 CheckId 至多一条；没有 observation 才表示 missing，不能选择性隐去 failed/cancelled，也不能把旧 generation 的 attempt 复制进当前轮。`approved` 对每个 required CheckId 必须恰有一条 latest current passed observation。

Planner 检查建立初始集合；Reviewer 只能追加。Reviewer 调用一个尚未纳入集合的 `cargo_check|cargo_test` 时，core 在执行前先把其规范化 selector 原子加入集合并发布 queued snapshot，因此 Reviewer 不能运行失败检查后在提交时隐去它。Reviewer 也可在 `submit_review` 中声明尚未运行的新检查：`changes_requested` 可以把它留给下一轮 Executor，`approved` 则只有该检查已有 current passed evidence 时才接受。累积数量超过 16 的调用批次零执行。

Executor 每次 `submit_execution` 前必须补齐当前 generation 的全部检查。`TestSnapshot` 不是“最后一条 Cargo 命令”，而是当前 generation 的累积 required-check 聚合：每个 CheckId 恰有一个 case；workspace 或集合变化时缺失/旧 evidence 显示 queued。aggregate priority 固定为 `running > failed > cancelled > queued > passed`，且 passed 当且仅当全部 case passed。集合追加即使 generation 不变也必须发布新 snapshot。

wire 投影固定按 required-check ledger 的 core-assigned 创建顺序排列 cases；`TestCaseDto.id = CheckId`。name 不拼 shell command，而使用规范化显示串：`cargo_check[package=<package|workspace>]` 或 `cargo_test[package=<package|workspace>;integration_test=<name|all>]`。queued/running case 的 `duration_ms=0`，summary 分别固定为 `Awaiting current-generation evidence` / `Check is running`；terminal case 使用 latest observation 的 status、duration 和脱敏摘要。REST、持久 panel event、SSE replay 与 UI 都只能调用同一个 projector，不能各自重建。

### 8.4 Reviewer diff 覆盖证明

Reviewer 的 approved 不只要求 core 在终态看见完整 diff，还必须证明该 Reviewer transcript 已接收当前 generation 的完整、未截断 diff。Reviewer 首先调用 `review_diff_manifest`，得到 `{generation, digest, files[], chunk_count}`；manifest 按规范路径排序，每个文件含 status、additions、deletions、patch byte count 和 chunk range。随后 core 通过 typed required action 强制该 role run 依序调用 `review_diff_chunks {generation, digest, start_chunk, count}`，每批最多连续两个 chunk。

manifest/chunk 边界来自同一稳定 terminal-diff 表示；`manifest_sha256` 对带固定 domain separator `coding-agent-review-diff-manifest-v1\0` 的 canonical manifest bytes 求 SHA-256。v1 的排序、字段编码和 chunk 切分不得静默改变，未来格式使用新版本标识。

- manifest 的完整、脱敏后 protocol-wrapped retained ToolResult 最多 24 KiB；
- 每个 chunk 在同一 wrapper-inclusive 口径下最多 20 KiB，每个 batch retained ToolResult 最多 40 KiB；
- 每轮最多 8 个 chunk；
- chunk 序号、checkpoint 和内容范围由 runtime 生成，provider 参数必须精确匹配；
- 每个成功、未截断 chunk 的 ToolResult 已进入下一次 Reviewer provider request 后才标记 covered；仅 runtime 返回而尚未发送不构成模型可见覆盖，重复 chunk 不增加覆盖；
- workspace 变化立即清空当前 coverage；
- binary、无法安全表示的路径/patch、manifest/chunk 超限或超过 8 chunks 时不能 approved，并以稳定 coverage-limit failure 结束为 Unreviewed，而不是让 Reviewer 凭摘要批准。

`ReviewCoverageEvidence { generation, workspace_digest, manifest_sha256, covered_chunks, total_chunks }` 随 review evidence 持久化。`approved` 要求 covered_chunks 精确覆盖 `0..total_chunks`；`changes_requested` 可以在发现 blocking 问题后提前结束，不要求完整覆盖。Reviewer 可使用普通 `git_diff/read_file` 辅助调查，但它们不能替代上述 coverage evidence。进入 Reviewer 时预算必须保守预留一次 manifest、最多四次 chunk-batch 和 terminal control；在 10-response role ceiling 下至少仍保留四次普通模型响应。

### 8.5 Reviewer 命令改变 deliverable

若 Reviewer 的任何 Cargo 或其他允许命令改变 deliverable fingerprint：

1. checkpoint 递增 generation；
2. 清空全部旧检查 evidence；
3. 当前 Reviewer transcript 和任何 provider verdict 失效；
4. core 生成一条 blocking finding，说明审查验证改变了可交付工作区；
5. 本轮以 `decision_source=system` 的确定性 `changes_requested` review evidence 持久化并计入三轮上限；不得把它标记为 Reviewer 自己提交的裁决；
6. 先 flush 新 generation 的 diff 和 queued aggregate tests 并取得 durable ack；
7. 若尚有返工轮次则返回 Executor，否则最终为 `Failed + ReviewRejected`。

该系统 finding 不包含原始命令输出、绝对路径或秘密。只改变 ignored build artifact 不影响 deliverable fingerprint，也不触发本规则。

### 8.6 最终批准

Reviewer 提交 approved 后，编排器再次采集稳定 fingerprint 与未截断 terminal diff。只有以下全部成立才产生 approved runner outcome：

- checkpoint generation 与 Reviewer evidence generation 相同；
- digest algorithm/value 相同；
- approved coverage 完整，且 terminal diff 重新计算的 manifest SHA-256 与 coverage.manifest_sha256 相同；
- 累积 required checks 非空、包含 cargo_test，且全部有同 checkpoint passed evidence；
- terminal diff 完整，未触发 Project 2 的截断拒绝；
- worktree identity 与 attempt artifact 仍匹配；
- cancellation 尚未生效。

任何不一致都不能自动批准。若 approved 尚未持久化而终态采集发现 workspace 变化，当前 round 不写 approved evidence，而是转换为 8.5 的唯一一条 system-generated `changes_requested` evidence；同一 `(task_id, round)` 绝不能同时出现 approved 和 rejected 两条记录。若仍有 review 轮次则返工，无轮次则拒绝。证据采集、store 或身份错误以相应 failure 结束为 Unreviewed，不伪装为 ReviewRejected。

在返回 final runner outcome 前，EventProjection 必须取消/收束 debounce，按 generation 发布最新完整 `diff.updated` 与上述聚合 `test.updated`，并等待两者的 durable ack。flush 失败不能继续质量事务，只能进入 `Failed + Unreviewed` 的持久恢复路径。因而最终可见事件顺序固定为 `diff.updated/test.updated -> review.updated -> terminal lifecycle`；同一 generation 下 diff 与 tests 的相对顺序可以固定实现，但必须测试且不能晚于 review。

## 9. 任务级预算与收敛

### 9.1 硬上限

生产默认共享硬预算：

| 范围 | 模型响应 | model-visible calls |
|---|---:|---:|
| 整个 Task | 60 | 96 |
| Planner | 8 | 12 |
| 每轮 Executor | 20 | 32 |
| 每轮 Reviewer | 10 | 16 |

角色 ceiling 总和可以超过 Task ceiling；共享 `TaskBudgetLedger` 永远先于角色 ceiling 生效。所有加法、预留和转换使用 checked arithmetic。control action 与 runtime action 都计入 call 数；无效但已收到的 provider response 计入 model response 和 provider byte 数，副作用为零。

Provider 边界保持：

- 每个 HTTP request 最多 1 MiB；
- 每个 HTTP response 最多 1 MiB；
- 整个 Task 的 encoded provider request + response 累计最多 8 MiB；
- 单个送回模型的 tool result 最多 256 KiB；
- 整个 Task 实际保留并送回模型的 tool-result bytes 累计最多 768 KiB；
- Planner role lease 最多 128 KiB，每轮 Executor/Reviewer role lease 各最多 256 KiB，所有 lease 同时受 768 KiB 共享总账约束，不因新 role run 重置。

Project 2 的 `max_tool_result_bytes` 实际是单循环累计上限；Project 3 必须把“单结果截断上限”和“任务/角色累计账本”拆成不同字段，不能把原字段误当成每调用 256 KiB 后把最坏 transcript 放大 96 倍。计费使用脱敏、协议 wrapper 后实际保留的 UTF-8 字节；被截断部分不进入模型但 `truncated=true`，diff coverage chunk 一旦截断则不计 covered。

`ChatCompletionsClient::start_task()` 整个 Task 只调用一次。每个角色不能获得新的 8 MiB provider 账本；core 的 provider byte 统计和 provider client 的任务级限制必须覆盖相同的角色集合。

### 9.2 阶段预留

进入角色前先原子检查共享剩余额度是否能容纳该阶段已知的强制动作：

- Planner：至少一次 `submit_plan` 或 `report_blocked`；
- Executor：所有当前缺失 required checks、一次 `submit_execution|report_blocked`，以及紧随其后 Reviewer 的完整强制路径（一次 manifest、最多四次 chunk-batch、一次 terminal control，共预留 6 responses + 6 calls）；
- Reviewer：固定预留一次 manifest、最多四次精确 diff chunk-batch 和一次 `submit_review|report_blocked`，共 6 responses + 6 calls；不重新执行 Executor 已提供的 current passed checks；
- finalization：不需要 provider 调用，但保留有界 fingerprint、diff 和 event/store 时间预算。

普通探索调用只有在执行后仍保留当前阶段强制 call/response 数时才允许。Executor required check 逐项强制时，每项至少保留一个 provider response 与一个 call；允许 provider 在普通阶段批量提出独立读取，但不能用潜在 batching 减少保守预留。Reviewer 默认复用与当前 checkpoint 绑定的 Executor evidence；它运行或新增检查属于可选动作，只有执行后仍保留 coverage + terminal response/call 时才允许。Reviewer 可在 changes_requested submission 中追加尚未运行的检查交给下一轮 Executor。

开始 Executor 前，Task ledger 必须为紧随其后的 Reviewer 原子保留 184 KiB；Executor 普通/validation 结果不能消费它。进入 Reviewer 时把这份 reservation 移入其 256 KiB role lease，普通结果仍不能消费 coverage reservation；manifest 返回实际 chunk_count 后可只释放不再需要的 batch 差额。若 Reviewer 提前提交有效 changes_requested，未使用的 reservation 随 role 结束释放，但已经实际计入 Task 总账的字节不退还。validation 的模型可见结果另固定为最多 8 KiB/次，权威 CheckEvidence summary 仍最多 2,048 bytes，以便按缺失检查数做保守 byte 预留，同时给 Executor 留出编辑/检查输出空间。

Tool-result ledger 对每个脱敏、wrapper-complete retained result 只计一次；该结果在后续 transcript 中重复编码不重复计 tool-result ledger，而是每次随完整 request 进入 8 MiB provider byte 账本。任何阶段无法同时取得 call/response 与 byte reservation 时，在启动该阶段前以对应 task-budget failure 结束。

Provider 字节无法在阶段开始时准确预测，因此每次请求前按当前 canonical encoding 计费，并为该次受限 response 检查剩余硬上限；不足时以当前阶段 context-limit failure 结束。不能静默新建 provider session、截断结构化权威对象或借下一角色重置字节账本。

若 Reviewer 请求返工但剩余 Task/role 额度不足以启动下一个 Executor 和 Reviewer 的已知强制动作，任务以阶段化 budget failure 结束为 `Failed + Unreviewed`，而不是将该轮升级为 ReviewRejected。

## 10. Transcript、tool_call_id 与 Provider 协议

### 10.1 独立 transcript

每次 `Planner #1`、`Executor #N`、`Reviewer #N` 都从新的 `[system, user]` 消息数组开始。角色间允许传递的对象仅为：

- 原始 Task 和受限 repository context；
- 验证后的 Planner plan；
- 当前 checkpoint 与 required check/evidence 摘要；
- Executor 的 bounded submission summary；
- 先前 ReviewRound 的 bounded structured summary。

禁止传递其他角色的 `AssistantToolCalls`、`ToolResult`、assistant final text、reasoning content、request ID、usage metadata 或 provider 原始 JSON。provider client 可以共享连接、配置和 Task byte ledger，但不能保存或隐式注入跨角色历史。

每次角色 handoff 使用固定字段顺序和规范化 CheckId/round 排序，canonical JSON 最多 256 KiB。先前 review 只传 verdict、decision source、findings 和 check additions 的有界领域摘要，不重复嵌入旧 check output、coverage chunks 或 diff；当前 check ledger 单独投影。handoff 超限以当前角色 context-limit failure fail closed，不能截断权威 finding 或偷偷省略某轮。该上限仍受单请求 1 MiB 和 Task 8 MiB provider 总账约束。

### 10.2 调用身份和批次

provider 的原始 `tool_call_id` 在一个 role run 的 transcript 内必须非空、有界且唯一。core 内部身份为 `{role, role_run, tool_call_id}`；不同角色或不同返工轮次可以收到相同 opaque ID，但绝不能互相引用。Activity/Event ID 另由 task-global monotonic generator 产生，不能因新建角色 loop 从 1 重新开始。

每个非空批次先整体检查角色权限、action schema、ID、secret stability、剩余预算和 control/runtime 互斥；任一失败使整批零执行。合法 runtime 批次按 provider 数组顺序串行执行，结果以相同 ID 和顺序加入当前角色 transcript。不能并行执行、排序、拆分后部分执行或把一批调用移交下一角色。

### 10.3 Tool choice 兼容

Provider 继续支持 Project 2 已批准的 strict、`required_as_required` 和 `required_as_auto` 三种模式。`ModelToolChoice::RequiredCargoTest` 应泛化为 typed `RequiredAction`，其中 validation action 携带 canonical CheckId、kind 和完整 package/integration-test selector，diff coverage action 携带精确 generation/digest/start/count，terminal action 携带预期 control kind。provider schema 对参数使用 `const`/single-value enum，core 在执行整批前仍对解码后的参数做逐字段精确匹配；仅调用同名 `cargo_test` 但使用另一 selector 不能满足预留检查。

- strict：发送命名 tool choice 和精确参数 schema；
- required compatibility：只暴露目标 action schema 并发送 `required`；
- auto compatibility：只暴露目标 action schema并发送 `auto`，但本地仍要求 exactly one matching call。

在 auto compatibility 下返回 final text、不同 action、零调用或多调用均 fail closed；不能重试后自动切换兼容模式。角色允许的 schema 必须由 provider 请求按 role 精确生成，core role wrapper 再做第二层授权。

### 10.4 Reasoning 与可见事件

同一 role run 内，provider 为继续 thinking-mode tool turn 所需的 opaque reasoning content 可按 Project 2 规则原样 round-trip，但不计为用户可见文本、不进入领域对象、不写事件或日志。角色切换时必须丢弃。Activity 只包含 core 生成的阶段动作、受控摘要和稳定错误，不包含 chain-of-thought、raw provider messages 或任意 JSON。

## 11. 领域模型、事件与投影

### 11.1 DeliveryReadiness

新增：

```text
DeliveryReadiness = Unreviewed | ReviewApproved | ReviewRejected
```

serde/wire 值为 `unreviewed|review_approved|review_rejected`，默认是 Unreviewed。`Task::try_from_stored` 验证 readiness/TaskStatus 矩阵；旧 lifecycle payload 缺字段时通过 serde default 解码为 Unreviewed。不能通过迁移把旧 Completed 回填为 Approved。

### 11.2 扩展计划和活动

`PlanSnapshot` 增加 `format_version`、summary、initial_required_checks；`PlanItem` 增加 description、acceptance_criteria。旧事件缺失字段时以 `format_version=0` 和安全空值解码；Project 3 的结构化 Planner 输出固定为 `format_version=1` 并满足第 6 节非空约束。provider/core submission validator 对 v1 使用严格规则，wire/SSE validator 则接受合法的 legacy v0 空字段，不能让历史 plan 造成断流。UI 对 v0 显示“历史计划未记录结构化验收条件”。

v1 `PlanSnapshot.revision` 是 checked、单调递增的计划投影版本，不是 workspace generation。v0 历史 revision 保持 opaque display value，不要求严格递增，也不重新解释 Project 2 已持久化的数值。

`ActivityEntry` 增加：

```text
actor = system | planner | executor | reviewer
role_run = null | positive integer
```

`actor` 和 `role_run` 在 wire 上都是 required 字段，其中 `role_run` required-nullable：legacy/system 必须为 null，planner/executor/reviewer 必须为正整数。旧 Activity 通过 serde default 得到 `system + null`。Project 3 的 role activity 使用 task-global 唯一 ID；不能让每个 role projection 重新使用 `coding-agent-1`。

`DiffSnapshot.revision` 和 `TestSnapshot.revision` 在 Project 3 正式定义为 workspace generation。wire 字段名为兼容保留 `revision`；UI 文案显示 Workspace generation。

### 11.3 ReviewEvidence

每轮不可变 evidence 至少包含：

```text
ReviewEvidence {
  round,
  decision_source: reviewer | system,
  workspace_generation,
  workspace_digest{ algorithm, value },
  verdict,
  summary,
  findings[],
  added_required_checks[],
  required_checks[],
  check_evidence[],
  coverage?,
  created_at
}
```

普通 `submit_review` 只能产生 `decision_source=reviewer`；只有第 8.5/8.6 节定义的 checkpoint invalidation 能产生 system decision，且 system decision 只能是 changes_requested、必须只有 core-generated blocking finding。UI 和持久层不得将两者混同。

ReviewEvidence 的 canonical JSON（包含 JSON escaping 后的实际 UTF-8 编码）最多 128 KiB；完整 `review.updated` wire event 编码最多 192 KiB，严格低于前端 256 KiB SSE frame 上限。构造时加入 core-generated CheckEvidence 后再次测量，超限 fail closed，不能截断 finding、selector 或权威 evidence。边界测试必须覆盖最大 Unicode escaping。

`TaskDetail` 增加按 round 升序的 `reviews[]`。新增 `review.updated` / `ReviewUpdated { review }` 持久事件；继续使用 event envelope `schema_version = 1`，因为这是向现有 tagged union 增加可忽略 variant，而不是改变既有 v0/v1 payload 的已持久字段。旧客户端遇到未知 v1 kind 必须记录 diagnostic、推进 cursor 并安全忽略；新客户端必须完整验证该 payload。

权威 review 不能藏在 `activity.message`。`task_review_evidence` typed row 是唯一事实源；review event 的数据库 payload 只保存固定 `{"evidence_ref":true}` marker。store 读取 event 时按 `(event_id, task_id, event_kind)` 复合关系 join evidence row，round 从该 row 取得，再构造 wire `ReviewUpdated { review }`。TaskDetail.reviews 同样由 typed evidence rows 投影，不能从第二份独立 JSON 复制品重建。Activity 可同步给出简短可读提示，但 quality gate、SSE replay 和 Project 4 合并门只读取 typed evidence/readiness。

### 11.4 投影顺序

非终态 `changes_requested` 的 `review.updated` 在 Task 仍 Running 时发布。最终 generation 的 diff/test durable ack 必须更早；最终 approved/rejected 的事务再按 event ID 顺序写入：

1. `review.updated`
2. 对应的 `task.completed` 或 `task.failed`

`Task.last_event_id` 指向 lifecycle terminal event。snapshot transaction 的全局 high watermark 与 SSE buffer/回查语义沿用 Project 1；一个增量客户端可以先收到 approved review、后收到 terminal lifecycle。仅处理前一个 event 时，reducer 不得自行改变其已有的 Task status/readiness，而显示“Reviewer 已裁决，等待终态事件”；final Store 事务其实已经原子提交，因此此时新取得的 REST/bootstrap 快照可以合法直接返回 `Completed + ReviewApproved`。数据库快照绝不能出现 terminal readiness 没有对应 evidence 的状态。

## 12. SQLite v3 与原子写入

### 12.1 表结构

schema migration v3 新增两类数据，不把权威证据只塞进 `tasks` 枚举列。

`task_review_evidence` 是 review 的唯一事实源，每轮一行，至少包含：

- `task_id`、`repository_id`、`attempt`；
- `review_round`，约束 1..=3；
- `workspace_generation`，0..=9,007,199,254,740,991；
- `digest_algorithm`、`workspace_digest`；
- `decision_source`、`verdict`；
- typed/canonical `summary`、`findings_json`、`added_checks_json`、`required_checks_json`、`check_evidence_json`、`coverage_json`（JSON null 或 object）；
- `created_at`、唯一 `event_id` 和固定为 `review.updated` 的 `event_kind`；
- `PRIMARY KEY(task_id, review_round)`；
- `UNIQUE(task_id, review_round, verdict)` 供 delivery verdict-aware foreign key 使用；
- `(task_id, repository_id, attempt) -> tasks(id, repository_id, attempt)` 复合外键；
- `(event_id, task_id, event_kind) -> task_events(id, task_id, kind)` 复合外键；后者要求 v3 为 parent tuple 建 UNIQUE index。

新表使用 SQLite `STRICT` 或等效显式约束。所有身份、round、generation、digest、decision source、verdict、JSON、时间、event ID/kind 均为 `NOT NULL`；round/generation/event ID 要求 `typeof(...)='integer'` 和各自范围；文本要求 `typeof(...)='text'`。所有 JSON 列在 STRICT 表中声明为 `TEXT NOT NULL`；`coverage_json` 的无覆盖值是 canonical JSON 文本 `null`，不是 SQL `NULL`。JSON 列只能由固定领域类型 canonical serialize/deserialize，必须 `json_valid`、具有预期 object/array/null `json_type`，并执行第 6/11 节数量和 128 KiB evidence 总上限；不能接受任意 provider JSON。digest algorithm 固定允许已知值，v1 value 必须 `length=64` 且不含 `[0-9a-f]` 以外字符。

DDL 必须同时包含 `decision_source IN ('reviewer','system')`、`verdict IN ('approved','changes_requested')` 和 `decision_source != 'system' OR verdict = 'changes_requested'` 的 CHECK；typed domain/Store decoder 还要验证 system evidence 只有第 8.5/8.6 节规定的一条 core-generated blocking finding。不能让 `system + approved` 仅凭 delivery FK 成为合法数据。

`task_events.payload_json` 对 `review.updated` 只保存固定非空 marker `{"evidence_ref":true}`，不保存第二份 evidence 或可漂移的 round。这样现有 lifecycle 事务仍可独占使用 `{}` 作为未回填 payload 的故障检测占位符。唯一 event_id 通过上述复合关系定位 typed row；store event query 必须 join 对应 evidence，typed decode 后构造领域/wire event。缺 row、kind/task 不同或非法 typed row 一律视为数据库不变量破坏。通用 `append_running_event` 不接受 ReviewUpdated；它只能由专用 `record_review` 或 finalization 操作创建，避免孤儿事件。

`task_review_evidence` 是 append-only：v3 为该表安装禁止 UPDATE 和 DELETE 的 abort triggers，生产 Store API 也不暴露修改/删除入口。最终 `task_delivery_state` 同样只允许一次 INSERT，并以 triggers 禁止 UPDATE/DELETE；重试创建新 Task，不改旧 Task 的交付裁决。raw-SQL corruption fixtures 必须证明读取在缺 row、非法组合或被绕过的突变上 fail closed。

`task_delivery_state` 每个已最终决定的 Task 至多一行：

- `task_id` primary key；
- `readiness` 只允许 `review_approved|review_rejected`；
- `final_review_round`；
- `final_verdict`；
- `decided_at`；
- `(task_id, final_review_round, final_verdict)` 外键引用 review evidence 的对应 UNIQUE tuple；
- CHECK 固定 `review_approved -> approved`，以及 `review_rejected -> changes_requested && final_review_round=3`。

缺少 delivery-state 行即 Unreviewed。这样旧 Task 天然兼容，Approved/Rejected 必须沿外键取得正确 verdict 的最终 generation/digest，Project 4 不能只信孤立枚举。所有 Task 读取——bootstrap、list、detail、create/retry/cancel mutation response 和 lifecycle event 构造——必须复用同一个 `tasks LEFT JOIN task_delivery_state` typed mapper；只有缺 row 才映射 Unreviewed。加载 aggregate 时还要验证 Approved 对应 Completed/no failure，Rejected 对应 Failed/`REVIEW_REJECTED`。

### 12.2 非终态审查事务

Runner 只提交不含数据库时间/event ID 的 `NewReviewEvidence`；Store 在首次成功事务中生成一次时间，并将同一值用于 evidence.created_at 与 event envelope.created_at。最终事务还把这同一时间用于 delivery.decided_at、Task.finished_at 和 lifecycle envelope。round 1/2 的 changes_requested 使用一个 `BEGIN IMMEDIATE` 事务：

1. 先查询 `(task_id, review_round)`；若存在，按下述幂等规则返回；
2. 仅在不存在时验证 Task 仍为预期的 Running/attempt/repository，且 round 连续；
3. 插入固定 `{"evidence_ref":true}` marker 的 `review.updated` event；
4. 插入引用该 event tuple 的 immutable evidence；
5. 更新 Task.last_event_id；
6. commit 后才唤醒 dispatcher。

幂等比较只覆盖调用方拥有的 canonical evidence 字段，不比较 Store 生成的 created_at/event ID。既有 row 与请求完全同值时，即使 Task 后来已终态，也返回 Existing 和原 event ID，并重新触发有界 dispatcher flush；同 key 异值、只有 event 没 evidence、操作类型不匹配、round 跳跃或 attempt 不匹配均为 typed conflict，不能追加孤儿事件。

### 12.3 最终质量事务

TaskManager 继续是唯一 Task terminal owner。`CodingAgentRunner` 返回带 `NewReviewEvidence` 和稳定请求字段的 `Approved`、`Rejected`、普通 Failed 或 Cancelled outcome，不能像 Project 2 那样把 completion 降成无负载 `Succeeded`。

v3 必须从通用 `TaskTransition`/`transition_with_event` 删除或永久拒绝无 evidence 的 `Completed` 分支。新的 `Running -> Completed` 只能由 `finalize_reviewed_task` 专用 StoreWriter 操作产生；历史 Completed + Unreviewed 可以读取和 retry，测试 fixture 若需构造历史状态必须使用 migration fixture 或显式 test-only helper，生产接口不可调用。

Approved/Rejected 使用一个 `BEGIN IMMEDIATE` 事务：

1. 先查询 final `(task_id, round)` evidence、delivery row 和 Task；完全同值且 terminal tuple 完整时返回 Existing 及原 terminal event ID 并重新 flush；
2. 既有 key 异值或 evidence/delivery/terminal 只提交一部分属于 invariant conflict；
3. 仅在不存在既有 final evidence 时验证 Task 为预期 Running attempt、所有先前 round 连续且 final evidence 满足状态矩阵；
4. 插入固定 `{"evidence_ref":true}` marker 的 final `review.updated` event 和 typed evidence；
5. Approved 时 Running->Completed；Rejected 时 Running->Failed，并写稳定 `REVIEW_REJECTED` failure；
6. 插入与 Task 终态一致的 `task_delivery_state` 并通过 verdict-aware FK 引用 final round；
7. 追加包含最终 readiness 的 lifecycle event；
8. 将 last_event_id 指向 terminal event；
9. commit 后以最后 event ID 唤醒 dispatcher。

任一步失败整体回滚；中间的 Task status update 对事务外不可见。不能先提交 ReviewApproved 再完成 Task，也不能先发布 completed 再补 evidence。writer reply 丢失后的重放遵守“先查 Existing、后验证 Running”；PendingDurableResult 保存相同 NewReviewEvidence 请求，Store 生成字段不参与比较。已终态但 tuple 不同是 conflict。

### 12.4 迁移兼容

v1->v3 与 v2->v3 migration 都必须验证真实旧数据库 fixture，并在升级后运行 `PRAGMA foreign_key_check`：

- 旧 Queued/Running 在恢复时仍按 Project 1/2 规则 Interrupted；
- 旧 Completed/Failed/Cancelled/Interrupted 均没有 delivery row，映射 Unreviewed；
- 旧 lifecycle JSON 和 plan/activity payload 可通过 serde default 重放；
- v1 跳级升级正确创建 v2 artifact schema 但不伪造历史 artifact row；v2 fixture 的既有 artifact row 不漂移；
- 不伪造 review evidence；
- 重复 migrate 无变化；
- v3 失败时同一次 open 中未提交的 migration 全部回滚。

## 13. 恢复、取消与幂等

应用重启不恢复 provider transcript 或继续角色。`recover_incomplete` 仍把 Queued/Running Task 转为 Interrupted；已持久化的 Planner plan、activity、diff/test 和中间 review evidence 保留，但 readiness 仍 Unreviewed。若最终质量事务已 commit 而 dispatcher wake 丢失，Task 已是 terminal，现有 SQLite 回查必须补发两个有序事件，不能改成 Interrupted。

如果最终事务尚未 commit，review/delivery/terminal 全部回滚，恢复只写 Interrupted。不能从 worktree 现场猜测 Reviewer 是否批准，也不能根据最后一条 activity 补写 readiness。

运行中的 cancel：

- cancellation token 传入每个 role/provider/runtime/fingerprint/event wait；
- 终止当前进程树并阻止后续角色；
- terminal diff/snapshot 沿用 Project 2 的 10 秒总收尾预算；
- 不写 final review 或 delivery state；
- Task 结束为 Cancelled + Unreviewed。

冷启动与进程内 degraded recovery 必须区分：

- 冷启动不存在内存 PendingDurableResult，直接对仍 Queued/Running 的 Task 执行 `recover_incomplete -> Interrupted`；已经 commit 的 final tuple 是 terminal，只由 dispatcher 回查补发。
- 进程内 store degraded 且提交结果未知时，先冻结新命令，保留稳定、typed、按原顺序的 review/final PendingDurableResult；Store 恢复后先通过专用 writer 幂等重放全部 pending，再仅对仍 Queued/Running 的剩余 Task 执行 `recover_incomplete`，最后 flush 到当前最大 event ID，之后才恢复 Ready。
- replay 的 SQLite 逻辑 conflict 是不变量故障并保持 frozen；暂时不可用继续 degraded 重试。若进程在 replay 前退出，内存 pending 丢失，下一次冷启动安全地采用上一条规则，不从 worktree 猜测结果。

用户 retry 保持 Project 1/2 语义：创建新 Task ID、新 attempt/worktree、新 provider session，readiness=Unreviewed、reviews=[]、generation 从新 worktree 的 0 开始。旧 Task 的审查历史不可复制为新批准证据。

## 14. 稳定失败语义

Project 2 已有失败码继续适用于 worktree、provider、runtime、fingerprint、terminal diff 和 event sink。Project 3 新增稳定类别；具体 message 仍为固定、脱敏、非路径文本：

- `PLANNER_INVALID_OUTPUT`
- `EXECUTOR_INVALID_OUTPUT`
- `REVIEWER_INVALID_OUTPUT`
- `PLANNER_ACTION_NOT_ALLOWED`
- `EXECUTOR_ACTION_NOT_ALLOWED`
- `REVIEWER_ACTION_NOT_ALLOWED`
- `PLANNER_STEP_LIMIT_REACHED`
- `EXECUTOR_STEP_LIMIT_REACHED`
- `REVIEWER_STEP_LIMIT_REACHED`
- `PLANNER_CONTEXT_LIMIT_REACHED`
- `EXECUTOR_CONTEXT_LIMIT_REACHED`
- `REVIEWER_CONTEXT_LIMIT_REACHED`
- `PLANNER_TASK_BUDGET_EXHAUSTED`
- `EXECUTOR_TASK_BUDGET_EXHAUSTED`
- `REVIEWER_TASK_BUDGET_EXHAUSTED`
- `{PLANNER|EXECUTOR|REVIEWER}_PROVIDER_FAILED`
- `{PLANNER|EXECUTOR|REVIEWER}_RUNTIME_FAILED`
- `{PLANNER|EXECUTOR|REVIEWER}_TIMEOUT`
- `{PLANNER|EXECUTOR|REVIEWER}_BLOCKED_{MISSING_CONTEXT|CONFLICTING_REQUIREMENTS|REQUIRES_GOAL_CHANGE|UNSUPPORTED_SCOPE}`
- `WORKSPACE_GENERATION_EXHAUSTED`
- `QUALITY_EVIDENCE_MISMATCH`
- `QUALITY_EVIDENCE_STORE_FAILED`
- `REVIEW_DIFF_COVERAGE_LIMIT`
- `REVIEW_REJECTED`

角色内部的 provider transport/rate/server、runtime 和 timeout terminal failure 分别映射到上述阶段化 code；底层稳定分类只用于安全内部诊断和决定 retryable，不动态拼进 wire code。角色开始前的 worktree/store 等 Project 2 failure 保留原 code。任何原始 provider body、模型内容、绝对路径、diff、命令输出或秘密都不能进入 TaskFailure。

`REVIEW_REJECTED` 是 retryable，因为用户可创建新 Task/attempt；invalid output、权限、step/context/task budget、coverage limit 和 evidence mismatch 默认 non-retryable；transport 等临时 provider 错误沿用 Project 2 retryability；blocked 中只有 missing context 为 retryable。

## 15. API、OpenAPI 与 React UI

### 15.1 Wire contract

wire enum 固定为：

```text
DeliveryReadinessDto = unreviewed | review_approved | review_rejected
ActivityActorDto = system | planner | executor | reviewer
ReviewDecisionSourceDto = reviewer | system
ReviewVerdictDto = approved | changes_requested
FindingSeverityDto = blocking | advisory
CheckEvidenceStatusDto = passed | failed | cancelled
```

最小 DTO exact shape：

```text
WorkspaceDigestDto {
  algorithm: "workspace_fingerprint_v1",
  value: string                         // exact lowercase 64 hex
}

CargoCheckDto {
  id: string,
  kind: "cargo_check",
  package: string | null                // required-nullable
}

CargoTestDto {
  id: string,
  kind: "cargo_test",
  package: string | null,               // required-nullable
  integration_test: string | null       // required-nullable
}

RequiredCheckDto = CargoCheckDto | CargoTestDto   // discriminator: kind

CheckEvidenceDto {
  check_id: string,
  actor: "executor" | "reviewer",
  role_run: integer,                    // positive
  workspace_generation: integer,        // 0..Number.MAX_SAFE_INTEGER
  workspace_digest: WorkspaceDigestDto,
  status: CheckEvidenceStatusDto,
  duration_ms: integer,                 // nonnegative
  summary: string,
  truncated: boolean
}

ReviewFindingDto {
  id: string,
  severity: FindingSeverityDto,
  message: string,
  path: string | null,                  // required-nullable
  line: integer | null                  // required-nullable; path required when non-null
}

ReviewCoverageDto {
  generation: integer,
  workspace_digest: WorkspaceDigestDto,
  manifest_sha256: string,              // exact lowercase 64 hex
  covered_chunks: integer[],            // sorted unique
  total_chunks: integer                 // 0..8
}

ReviewEvidenceDto {
  round: integer,                       // 1..3
  decision_source: ReviewDecisionSourceDto,
  workspace_generation: integer,
  workspace_digest: WorkspaceDigestDto,
  verdict: ReviewVerdictDto,
  summary: string,
  findings: ReviewFindingDto[],
  added_required_checks: RequiredCheckDto[],
  required_checks: RequiredCheckDto[],
  check_evidence: CheckEvidenceDto[],
  coverage: ReviewCoverageDto | null,   // required-nullable
  created_at: RFC3339 UTC string
}
```

`TaskDto.delivery_readiness`、`PlanSnapshotDto.format_version|summary|initial_required_checks`、`PlanItemDto.description|acceptance_criteria`、`ActivityEntryDto.actor|role_run` 和 `TaskDetailDto.reviews` 都是 required 字段；其中 Activity `role_run` 是 required-nullable。`ReviewUpdatedEventDto` 固定 payload `{ review: ReviewEvidenceDto }`。所有数组都 required，空集合编码为 `[]`，不能用 absent/null；除上面明确 nullable 字段外不接受 null。format_version 只允许 0|1，并执行第 11.2 节对应交叉约束。

Review DTO 还执行领域交叉约束：system source 只允许 changes_requested、coverage=null 和固定的一条 core blocking finding；其 added checks 只能来自 workspace 变化前已被 core 接受的 Reviewer tool invocation，不能来自未发生的 submission。reviewer approved 不得有 blocking finding且 coverage 必须完整；changes_requested 至少一个 blocking finding。

`required_checks` 中 CheckId 必须唯一，selector 必须 canonical，顺序必须等于 append-only ledger 顺序；`integration_test != null` 时 `package` 必须非 null。`added_required_checks` 必须精确等于本轮 required checks 相对 Planner initial checks/上一轮累计集合的有序增量，不能只是任意有序子集。`check_evidence` 的 CheckId 必须唯一、都引用 required checks，并精确投影提交时同 generation/digest 的 latest terminal observations；approved 时每个 required CheckId 恰有一条 status=passed 的 current evidence。coverage 的 generation/digest 必须等于父 ReviewEvidence，`covered_chunks` 排序且唯一，每个值满足 `0 <= chunk < total_chunks`；approved 时还必须精确覆盖全部 chunks。

校验职责按可见上下文拆分，不能要求单条 wire event 证明未携带的瞬时或历史状态：

- core 构造 ReviewEvidence 时持有 required-check ledger 与 current observation map，负责验证 added checks 的精确增量和 check_evidence 的精确投影；这是 failed/cancelled observation 不可被选择性隐去的写入不变量；
- Store typed row decoder 验证所有自包含 shape/bounds/enum/唯一引用/generation/digest/approved-evidence/coverage 关系；写事务和 aggregate loader 再利用持久 Plan 与前序 reviews 验证 round 连续、required checks 只增不减及 added checks 的精确增量。Store 不假称能从未单独持久化的 transient observation map 重新证明 changes_requested 是否遗漏 observation；
- SSE 单事件 runtime validator 只验证该 event 的自包含规则。Reducer 已持有完整 TaskDetail 历史时再验证跨轮增量；上下文不完整时将该 Task detail 标记 stale，推进正常 event cursor，并用 TaskDetail/snapshot 的 event_cursor 边界 refetch 后替换，不能凭不完整历史拒绝整个流；已有完整上下文却发现冲突时进入第 15.1 节的 bootstrap recovery；
- 完整 TaskDetail response validator 可用其中的 Plan 与全部 reviews 验证跨轮关系；OpenAPI 能表达的部分也必须编码进 schema。

各层的 corruption/validator 测试逐项覆盖自己能够证明的不变量，不能只依赖生产 happy path，也不能伪造不存在的独立 observation ledger。

不新增 REST endpoint；bootstrap、Task list/create/retry/cancel/detail 和 lifecycle SSE 中的 TaskDto 都由第 12.1 节同一个 LEFT JOIN mapper 产生 required readiness。Rust OpenAPI、手写 discriminator/oneOf、exported `web/openapi.json`、generated TypeScript、SSE runtime validator 和 reducer 必须在同一变更中更新。禁止用 `Record<string, unknown>` 代替 typed review/check payload。

旧客户端安全忽略未知 `review.updated` 并推进 cursor；新客户端必须将其加入 allowlist，并按 format_version 分层校验 legacy/new plan，以及校验 review 的全部 bounds/enum/shape。`(task_id, round)` 同 canonical payload 的 replay 被 reducer 忽略；同 key 不同 payload 触发 protocol diagnostic、停止该流的普通增量应用并进入 bootstrap recovery，不能静默覆盖或保留首值。客户端在 recovery 成功前不提交冲突 event 的 cursor，也不以旧 cursor 重开普通 SSE；它只按退避重试 snapshot/bootstrap。取得包含全局 high watermark 的一致快照后，以快照替换本地状态、把 cursor 原子推进到该 watermark、丢弃 buffer 中 `<= watermark` 的 events，再按 ID 应用更晚事件并恢复 SSE。这样快照覆盖冲突 event 而不会重连循环，也不会跳过请求期间提交的后续事件。

### 15.2 工作台

- Sidebar 保留 lifecycle badge，并增加紧凑 readiness badge。
- TaskWorkspace 分开显示 Execution status 和 Delivery readiness；历史 Completed + Unreviewed 明确显示“执行完成，尚未审查”。
- PlanPane 显示 Planner summary、步骤、description、acceptance criteria、状态和 initial required checks；Reviewer additions 与当前累积集合只在 ReviewPane 显示，不能回写或伪装成 Planner 产物。legacy format v0 显示结构化字段未记录。
- ActivityPane 显示 `System`、`Planner #1`、`Executor #N`、`Reviewer #N` 标签，不显示 transcript/reasoning。
- 右栏在 failure 与 diff 之间增加 ReviewPane，按 round 展示 decision source、verdict、workspace generation、digest 的短显示、summary、blocking/advisory findings、path:line、check additions、累积 checks、coverage 和 evidence。system invalidation 必须显示为系统裁决，不能标成 Reviewer 意见。
- Diff 文案把 Revision 明确为 Workspace generation；Tests header 同样显示 generation。
- 历史 review round 标记 historical/stale，不与当前 diff/tests 比较。Running + Unreviewed 时，latest changes_requested 若 generation 已因返工编辑推进，只标记 expected stale/historical，不报 integrity warning；若仍是同 generation 才与当前面板比较。latest approved 只可能处于等待 lifecycle 的增量瞬间，必须与当前面板比较。terminal Task 只比较 delivery state 外键实际引用的 final review。UI 无论如何都不改变服务端 readiness。
- 增量流中 final `review.updated` 已到但 lifecycle 尚未到时显示“Reviewer 已裁决，等待终态事件”，并保持 reducer 已有的 Task status/readiness；新的 REST/bootstrap 快照可以已包含原子提交后的终态，快照替换后不再等待 lifecycle event。
- 长 summary、finding、path 和 selector 必须 `overflow-wrap:anywhere`，面板保持 `min-width:0` 和既有窄屏折叠行为。

Project 3 不显示 merge、approve button、人工 override、worktree cleanup 或并发设置。

## 16. 测试策略

### 16.1 Core 状态机

- 首轮批准、一次返工批准、两次返工后批准、第三轮拒绝的完整序列。
- Planner 确实只运行一次，返工只在 Executor/Reviewer 间发生。
- 每个角色 blocked、invalid output、step/context/task budget、provider/runtime failure 的 Task 终态矩阵。
- 任意角色阶段取消以及批准事务前后的 cancellation race。
- Planner 写/Cargo 越权、Reviewer replace 越权时整批零执行。
- runtime/control 混合、多个 terminal control、空批次、重复 ID 和 redaction 变化时整批零执行。
- 角色 transcript 中不含其他角色的 calls/results/reasoning/metadata；同 ID 在不同 role run 可安全命名空间化。
- 全局预算不会因 role run 新建而重置；阶段预留和 checked arithmetic 覆盖边界。
- required checks 去重、至少 cargo_test、只增不减、current-generation 全部 passed 门禁；同 CheckId 重跑进入 queued/running 即撤销旧 passed，latest failed/cancelled 会替换它。
- typed RequiredAction 对 CheckId/package/integration-test 精确匹配；同工具名不同 selector 整批零执行。
- generation 的 same digest、A->B->A、external mutation、overflow 和 SQLite conversion 边界。
- Reviewer command 改变 deliverable 后系统 changes_requested、返工和第三轮 rejection。
- approved 前 manifest + 全 chunk coverage；缺 chunk、重复 chunk、截断、checkpoint 变化、binary/超限 diff 均不能批准。
- approved verdict 后 terminal fingerprint/diff/evidence 任一不一致都不能 Completed。
- cancel 与 Approved outcome 的两种 TaskManager mailbox 顺序均符合线性化规则。

### 16.2 Provider contract

- 每个角色只编码允许的 action schema，第二层 core wrapper 独立拒绝越权。
- 四种 terminal control 与 plan/review nested schema 的 exact JSON 编解码、unknown field、bounds 和 canonicalization。
- strict、required_as_required、required_as_auto 对 typed required action 的离线 contract 测试。
- auto 返回 final/zero/multiple/wrong action 时 fail closed，不发隐式 fallback 请求。
- reasoning content 只在同 role round-trip，角色切换清空。
- 一个 task session 的 8 MiB byte ledger 覆盖全部角色；per-request/response 和 secret redaction 保持。
- 单结果、role lease、768 KiB task tool-result ledger 与 diff coverage 预留不会在新角色重置。

### 16.3 Store 与恢复

- 空库 v3、真实 v1->v3/v2->v3、重复 migrate、`foreign_key_check`、失败回滚、JSON TEXT/null、decision-source/verdict CHECK 和 strict foreign-key/schema constraint。
- 旧 Completed 仍 Unreviewed，旧事件/DTO 缺字段可重放且不产生伪 evidence。
- 非终态 review 的 non-empty marker/event/evidence/last_event_id 原子性和同值幂等/异值冲突；evidence/delivery immutable triggers 与 raw corruption 读取 fail closed。
- final approved/rejected 在每个 SQL 步骤注入失败均全事务回滚。
- final event 顺序固定 review.updated 后 lifecycle，wake 使用最后 event ID。
- 通用 transition 无法产生新的 Completed + Unreviewed，finalize_reviewed_task 是唯一生产写入口。
- crash 前/后、reply/wake 丢失、先查 Existing 的幂等重放、degraded pending-first replay 和 cold-start recovery。
- Interrupted 保留中间 reviews 但不产生 readiness；retry 不继承证据。

### 16.4 App、API 与 UI

- 单 worktree、单 provider task session、全局真实并发仍为 1。
- RunnerOutcome 保留 checkpoint/final evidence 到 TaskManager，不再降成空 Succeeded。
- task-global activity ID 在七个可能 role run 中不碰撞。
- OpenAPI exact shape、11 种持久事件 variant、生成 TypeScript 无 drift。
- REST bootstrap/detail 与 SSE snapshot/live/replay/lag/reset 对 review.updated 幂等一致；同 key 异 payload 通过 snapshot high-watermark recovery 恢复且不循环/漏掉后续事件。
- approved/rejected/historical Task 在所有 REST 返回路径使用同一 readiness join mapper。
- 真实 v2 DB 经 REST/SSE 到 React 的 legacy plan/activity 兼容测试，不能因新 strict submission 规则断流。
- final panel flush 的事件顺序为 diff/test -> review.updated -> lifecycle，任何 debounce 不能越过 barrier。
- Sidebar/readiness、Plan、Activity actor、ReviewPane、瞬时 pending-finalization、generation mismatch、返工后首次编辑 expected-stale、历史 Completed disclaimer 的 Vitest。
- Playwright 覆盖首轮批准、一次返工、最终拒绝和旧 Completed 四个用户路径。
- 使用 scripted fake provider + 真实临时 Git/Cargo worktree 的 offline E2E，证明 Planner->Executor->Reviewer 的顺序和 digest gate。

所有默认测试离线、确定性、无真实 provider 凭据。真实 provider smoke 仍不是默认 CI 门，但可作为显式人工验证。

## 17. 验收门

Project 3 只有在以下全部成立时完成：

1. 用户确认本规格，且实现前另有逐步 TDD 计划。
2. 生产 runner 使用一次 worktree/session 驱动单次 Planner 和最多三轮 Executor/Reviewer；并发仍为 1。
3. 三角色权限在 provider schema 与 core/runtime wrapper 两层 fail closed。
4. 三角色 transcript、reasoning 和 tool-call batches 不共享、不重排，task budget 不重置。
5. 正常 Planner、Executor、Reviewer 只能通过验证后的 typed control action 结束阶段；唯一例外是明确持久为 `decision_source=system` 的 checkpoint invalidation。
6. 必需检查只增不减，最终 generation 的全部检查含 cargo_test 且有 current digest passed evidence。
7. Reviewer 最多两次返工；第三次 changes_requested 得到 Failed + ReviewRejected。
8. 只有 terminal checkpoint 一致和 Reviewer approved 才能原子得到 Completed + ReviewApproved。
9. 历史 Completed 和缺失 delivery state 的 Task 均显示 Unreviewed。
10. review evidence、delivery state、terminal Task 和事件不存在任何可注入的部分提交窗口。
11. 取消、恢复、blocked、预算、provider/runtime/store 错误均遵守终态矩阵且不伪造 rejected/approved。
12. OpenAPI、SSE、React 完整展示 readiness、角色活动、计划、审查历史和 generation 证据；前端不自行批准。
13. Project 3 不提供 merge、清理、真实并发、任意 shell 或 OS 沙箱承诺。
14. 新鲜验证至少通过：

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
```

随后按平台二选一执行 E2E；Windows PowerShell：

```powershell
$env:CODING_AGENT_E2E_BINARY = (Resolve-Path '.\target\debug\coding-agent-app.exe').Path
npm --prefix web run e2e
Remove-Item Env:CODING_AGENT_E2E_BINARY
```

Bash：

```bash
CODING_AGENT_E2E_BINARY="$PWD/target/debug/coding-agent-app" npm --prefix web run e2e
```

最后继续执行：

```text
cargo build --release --locked --offline -p coding-agent-app --features embedded-web
node scripts/check-placeholders.mjs
git diff --check
```

15. 独立代码审查确认高优先级问题已解决，最终 git diff 和测试证据已人工复核。

## 18. 实施顺序约束

实现计划必须按以下依赖顺序拆成可单独验证的 TDD 任务：

1. domain readiness、plan/review/check 类型与纯状态机；
2. SQLite v3、旧数据兼容、review/delivery 原子事务；
3. core TaskBudgetLedger、WorkspaceCheckpoint、required check ledger；
4. role-scoped action/control schema 与 provider contract；
5. Planner、Executor、Reviewer role engine 和 MultiRoleOrchestrator；
6. app RunnerOutcome、TaskManager/StoreWriter、degraded/recovery 集成；
7. event projection、API/OpenAPI/SSE；
8. React 工作台；
9. offline E2E、跨层故障注入、文档和全量验证。

每步先写失败测试，再做最小实现并运行相关测试；跨层 schema 改动必须在同一任务中更新生成物和消费者。实现期间不得开始 Project 4 的 merge、并发、清理、配额或发行行为，也不得以兼容为由保留可绕过 Reviewer 的 Project 2 生产成功路径。
