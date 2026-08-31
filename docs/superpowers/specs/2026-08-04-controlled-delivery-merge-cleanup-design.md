# Project 4B：受控本地交付、合并与 Git 现场清理设计

> 日期：2026-08-04
> 状态：P4-B 已完成实现、独立审查与完整验收；Project 4（P4-A + P4-B）已完成
> 验收日期：2026-09-01
> 基线：`29b81d9 project 4 P4-A：受控并发与资源准入`
> 范围：仅 P4-B；本地显式 merge、冲突恢复、已合并任务的 worktree/branch 清理。已批准的 Project 4 = P4-A + P4-B；P4-C/P4-D 是未来项目

## 1. 目标

P4-B 在 Project 3 的质量证据和 P4-A 的受控并发基础上，增加一条可恢复、可审计、默认不执行的本地交付路径：

1. 用户先看到合并资格、目标分支、目标 HEAD、证据版本和冲突预检。
2. 只有用户显式确认后，应用才把已审查工作区固化为 source commit。
3. 应用只向登记仓库当前 checkout 的 symbolic local branch 执行固定 `--no-ff` merge。
4. 冲突、外部 Git 漂移、子进程未知结果和 Store 回执丢失都必须收敛到明确状态。
5. merge 成功后，用户可以分别确认移除应用 worktree 和删除 source branch。
6. 默认永久保留 Git 现场；没有自动 merge、自动清理或按时间/配额删除。

P4-B 的成功标准不是“调用了一次 `git merge`”，而是：被批准的最终工作区、source commit、目标分支、merge commit、持久状态和 UI 投影之间存在可证明的一一对应关系；崩溃后可以依据持久意图和真实 Git 状态恢复，不猜测结果。

## 2. 已批准的产品边界

### 2.1 本阶段包含

- `Completed + ReviewApproved` 任务的合并资格查询。
- 对最终 review generation、workspace fingerprint、coverage/check evidence 和 attempt artifact 的重新认证。
- 交付专用的 dirty source worktree 观察能力。
- 使用临时 index 构造候选 tree，并固化为应用拥有的 source commit。
- 登记 checkout 当前 symbolic branch 的 clean/HEAD/Git-operation 预检。
- 固定 `--no-ff` 本地 merge。
- 无目标修改的冲突预检、冲突摘要和重新预检/重试。
- source commit、merge、abort、worktree unlock/remove、branch delete 的崩溃恢复。
- merge 后显式移除应用 worktree。
- worktree 移除后显式删除已经合并的 source branch。
- REST/OpenAPI/React 的预检、确认、进度、结果和恢复投影。
- SQLite v5 持久化、每个 POST 的独立 command receipt、幂等请求和 immutable operation transitions。

### 2.2 本阶段不包含

- 自动 merge 或因 Reviewer approval、启动恢复、页面刷新、Scheduler tick 自动触发 merge。
- rebase、cherry-pick、squash、fast-forward 策略选择或用户自定义 merge strategy。
- fetch、pull、push、远程认证、PR 创建或任何网络交付。
- 自动冲突解决、内置冲突编辑器或改写任务工作区。
- 自动 checkout/switch 目标分支。
- 用户自定义 commit author、message、hook、签名或 merge driver。
- 清理未合并、未审查、失败、取消、中断或 `Unreviewed/Rejected` attempt。
- force remove、`reset --hard`、`clean`、stash 或 `branch -D`。
- P4-C 的历史分页、搜索、真实 artifact 大小、保留期、配额、批量或自动删除。
- P4-D 的全链路脱敏重构、安装器、签名、公证、平台包装和真实 provider smoke。
- OS sandbox、阻止外部 Git、跨进程或分布式仓库锁。
- 扩展六态 `TaskStatus`，或把 delivery/cleanup 失败写成 `TaskFailure`。
- 增加第 12 种 persisted task event；既有 11 种 task event 和 SSE cursor 语义保持不变。

## 3. 对既有规格的显式修订与保持

### 3.1 保持不变

- `TaskStatus` 仍只有 `Queued/Running/Completed/Failed/Cancelled/Interrupted`。
- `DeliveryReadiness` 与 `TaskStatus` 分离。
- `Completed` 不代表已审查、可合并或已合并。
- 历史 `Completed + Unreviewed` 永远不能通过 P4-B 合并入口。
- `ReviewApproved` 仍表示最终 typed evidence 通过，不等于人工批准、生产安全或已交付。
- 一 Task、一 attempt branch、一 worktree 和一组最终证据的隔离边界保持。
- 原始 checkout 中的未提交文件不得被 reset、clean、stash、覆盖或推测性修复。
- `task_attempt_artifacts.ready` 仍只表示应用成功建立并认证该 attempt 的不可变 Git 身份，不承载 merge/cleanup 状态。
- P4-A `RepositoryControlCoordinator`、alias 认证、poison 和锁顺序继续作为唯一进程内 repository control 机制。
- SQLite transaction 不跨 runtime Git side effect。
- StoreWriter 不获取 repository lease，不等待 DeliveryManager mailbox。
- Agent/Planner/Executor/Reviewer 不获得 merge 或 cleanup 工具。

### 3.2 P4-B 的显式新增

- 新增与 Task 生命周期分离的 delivery source、merge operation 和 cleanup operation 状态。
- 新增一次性的本地 source commit 和固定 `--no-ff` merge commit。
- 新增仅面向交付的 dirty source worktree 认证路径，不放宽 P4-A 的 `open_ready/observe`。
- 新增 delivery-aware artifact reconciliation overlay：有 typed source/disposition 的 artifact 由 P4-B 状态解释 branch 前进、worktree 移除和 branch 删除；P4-A 原观察器只处理没有 P4-B ownership 的 artifact。
- delivery-owned artifact 不再进入要求 `branch=base + worktree present` 的 P4-A ready observation；所有启动、GET projection 和恢复入口先按同一 task/attempt join 判定 ownership，再路由到 P4-A 或 P4-B observer。缺失或矛盾 join 必须 fail closed，不能回退到旧 observer 猜测。
- 新增用户显式触发的 post-merge worktree/branch disposition。
- 新增独立 REST polling 投影；P4-B 不修改既有 persisted task event/SSE union。

## 4. 威胁模型与诚实边界

P4-B 面对的输入不只是 HTTP body。仓库路径、Git config、attributes、refs、index、worktree、外部编辑器和外部 Git 进程都可能在预检后变化。

P4-B 能保证：

- 应用自己的所有 mutation 都进入同一个 repository coordinator。
- 每个有副作用命令前后都重新认证 common Git identity、admin identity、branch、HEAD 和 operation state。
- HTTP 请求、Store receipt 和 Git child outcome 都有独立幂等/恢复证据。
- 已知冲突通常只产生 object database 中的临时对象，不改变目标 ref、index 或文件。
- 无法证明结果时不猜测成功或失败，而是 `ReconciliationRequired` 并 poison 受影响仓库。

P4-B 不能保证：

- 阻止用户或其他进程在应用命令前后运行外部 Git。
- 对不参与本应用 repository coordinator 的外部 Git writer 提供真实 index 的原子 compare-and-swap。最终 sampled revalidation 与 `read-tree --reset` 自己取得 Git index lock 之间仍可能有外部写入；若该写入在后续 observation 中可见则进入 `ReconciliationRequired`，但本阶段不能承诺检测已经被该固定 stage 覆盖的同一窗口写入或保留其 staging。
- 在外部进程与应用同时修改同一 checkout 时无条件自动恢复。
- 在磁盘、权限或设备故障后凭日志推断 Git side effect。
- 执行仓库自定义程序仍保持安全；因此本阶段直接拒绝 executable filter、hook 和 custom merge driver。

UI 和文档必须使用“进程内受控、外部漂移时 fail closed”，不能宣称全局 Git 锁或 OS 隔离。

## 5. 总体架构

依赖方向保持：

```text
web
  -> REST/OpenAPI
  -> coding-agent-api
  -> coding-agent-app DeliveryManager
       -> StoreWriter
       -> RepositoryControlCoordinator
       -> coding-agent-runtime delivery Git capabilities

coding-agent-store
  -> delivery source / merge / cleanup typed rows

coding-agent-domain
  -> TaskStatus / DeliveryReadiness / review evidence（不引入 Git）
```

### 5.1 crate 职责

`coding-agent-domain`：

- 不增加 Git、HTTP、SQLite 类型。
- 保持六态 Task 和现有 readiness/evidence 不变量。

`coding-agent-store`：

- migration v5。
- delivery source、merge、cleanup current row、immutable command receipt 和 transition row。
- eligibility 所需的一致 read snapshot。
- 单向状态、幂等 request hash、active uniqueness 和 transition transaction。

`coding-agent-runtime`：

- `DeliverySourceProvisioner`：认证已完成但 dirty 的 source worktree。
- `DeliveryGitRuntime`：临时 index/tree、commit-tree、merge-tree、merge/abort、unlock/remove、CAS ref delete。
- 所有命令为 typed `ValidatedCommand`，不接受 shell string。
- 路径、环境、输出、deadline 和 child cleanup 继续使用既有 capability/ProcessSupervisor。

`coding-agent-app`：

- `DeliveryManager` actor 拥有 operation orchestration。
- 获取 repository lease，提交 pending phase，执行 runtime side effect，再提交 observed outcome。
- 恢复、poison、shutdown、mutation gate 和 REST backend。

`coding-agent-api`：

- exact request/response/error/OpenAPI contract。
- session、Origin、CSRF 和 request bounds。
- 不接收任意 Git arguments、path 或 commit message。

`web`：

- Delivery panel、preflight modal、明确确认、polling、conflict summary 和两个 cleanup confirmation。
- 只消费 typed DTO，不解析 message、Git stderr 或日志。

### 5.2 源码拆分要求

实现必须遵守仓库 `AGENTS.md`：

- `DeliveryManager` orchestration、source commit、merge、cleanup、recovery 分文件。
- runtime 的 observation、tree construction、merge、cleanup 分模块。
- Store 的 schema DTO、query、transition 和 recovery query 分模块。
- API contract、handler、projection 和 error mapping 分模块。
- React 的 Delivery panel、confirmation、polling reducer 和 validation 分模块。
- 大方法只保留阶段编排；身份验证、状态转换、命令构造和 outcome classification 抽成单职责方法。

结构拆分不能引入 P4-C/P4-D 能力，也不能改变公开语义。

## 6. 合并资格

### 6.1 一致资格快照

Store 必须在一个一致 read transaction 中返回 `DeliveryEligibilitySnapshot`：

- Task 当前整行。
- 当前 attempt。
- `DeliveryReadiness`。
- 最终 review round、verdict、generation 和 workspace fingerprint。
- required checks/coverage manifest 的 typed evidence identity。
- 当前 attempt artifact 的 repository/base commit/branch/worktree/common Git identity/state。
- 已有 delivery source。
- 已有成功 merge、open preflight 或 side-effect-active merge operation。
- 已有 cleanup disposition/operation。

不能通过多次近似查询拼装资格。

### 6.2 必须同时成立

只有以下条件全部成立，preflight 才返回 `eligible=true`：

1. Task 为 `Completed`。
2. readiness 为 `ReviewApproved`。
3. 最新 review verdict 为 Approved。
4. review evidence 绑定当前 attempt 的最终 generation。
5. review workspace fingerprint、required checks 和 coverage manifest 均完整且内部一致。
6. artifact 为 `Ready`，repository/task/attempt/base/branch/path identity 全部匹配。
7. Task 不在 TaskManager active ownership 中。
8. 没有该 attempt 的存活或未知 process tree。
9. 没有 `ObjectPending|CommitPending|ReconciliationRequired` source、side-effect-active/reconciliation-required merge 或 active/reconciliation-required cleanup；同一请求的幂等重放除外。已有 `PreflightReady` 可按第 10.1 节由新 preflight 原子 supersede，已有 `PreflightPending` 只接受原 receipt 重放。
10. 若 source 已 `Committed`，其 evidence tuple 与当前最终 review 完全相同。
11. 该 Task 尚未成功 merge；成功 merge 只能查询/cleanup，不能再次 merge。

以下情况固定不可合并：

- `Completed + Unreviewed`。
- `ReviewRejected`。
- `Failed/Cancelled/Interrupted/Queued/Running`。
- artifact `Reserved/Inconsistent` 或不可观察。
- evidence generation/digest/manifest 不一致。
- process cleanup 无证明。
- repository poisoned。

本规格中的 merge operation 分类固定为：

- open preflight：`PreflightPending|PreflightReady`。
- side-effect-active：`Accepted|MergePending|AbortPending`。
- terminal：`Conflict|Rejected|Stale|Superseded|Failed|Merged`。
- `ReconciliationRequired` 单独阻断该 common Git identity 的后续 mutation，不归入可替换 terminal。

## 7. 交付源观察与 fingerprint 绑定

### 7.1 不复用 `open_ready`

P4-A `WorktreeProvisioner::open_ready/observe` 要求 attempt branch 仍指向 `base_commit` 且 worktree clean。真实 Completed 任务恰好以 dirty worktree 保存最终修改，因此 P4-B 必须增加独立的：

- `observe_delivery_source`
- `open_delivery_source`
- `observe_committed_delivery_source`

原方法的 clean/base 不变量不得放宽。

### 7.2 dirty source 的认证条件

`open_delivery_source` 必须证明：

- common Git platform identity 与 artifact 一致。
- worktree admin identity、path capability 和 source branch 一致。
- worktree 仍以应用固定 lock reason 处于 locked；外部 unlock/relock 或 reason drift 拒绝。
- `HEAD`/source ref 精确等于 artifact `base_commit`。
- symbolic branch 精确等于 artifact branch。
- index 不含 unmerged entries。
- worktree 不含 submodule/gitlink 变化。
- status/diff/untracked 集合在既有 bounds 内。
- 重新计算的 `workspace_fingerprint_v1` 精确等于最终 approved fingerprint。
- config/attributes 安全扫描通过。

dirty 是本方法的预期状态，但“任意 dirty”不是资格。任何额外文件、index 漂移或 fingerprint 不一致都返回 `DELIVERY_SOURCE_CHANGED`，不尝试修复。

### 7.3 临时 index

直接 `git add` 会改变 index metadata，并可能在读取 live worktree 时触发 filter/helper；它不能用于 candidate 构造。固定顺序为：

1. 保持真实 index 不变，完成最终 fingerprint 认证。
2. 从已认证 worktree 采集 no-follow、identity-bound snapshot，固定每个 approved tracked/untracked entry 的 raw path、mode 与 exact bytes；目录、symlink/reparse、identity 或读取不一致一律拒绝。
3. 创建应用私有临时 index capability，并从精确 `base_commit` 执行 `read-tree`。
4. 对 snapshot 的每个文件仅以固定 `hash-object -w --no-filters --stdin` 写入 exact bytes；再以 typed `update-index --add --replace -z --index-info`（`--index-info` 固定置于末尾）精确替换/移除临时 index entries，不读取 live 路径内容。
5. `write-tree` 得到候选 tree OID。
6. 再次认证真实 index/worktree并重新采集 snapshot/fingerprint，必须仍精确等于 approved fingerprint。
7. 持久写入 source `ObjectPending`，绑定 tree、parent、metadata 和 evidence tuple。
8. 生成并验证不可达 source commit object，持久写入 `CommitPending` 和 exact OID。
9. 才允许修改真实 index 或 source ref。

candidate 命令路径不得依赖仅重定向 worktree attributes 的保护；它必须结构上不调用 filter/helper，即使 `$GIT_DIR/info/attributes` 在认证后发生变化也没有 helper 执行面。临时 index 路径不进入日志/API，失败后只删除应用自己创建且身份匹配的临时文件。

## 8. source commit 状态机

```text
Absent
  -> ObjectPending
  -> CommitPending
  -> Committed

ObjectPending/CommitPending/Committed + 无法证明的身份或 child outcome
  -> ReconciliationRequired
```

### 8.1 `ObjectPending`

在第一个 source commit object side effect 前持久化：

- task/repository/attempt identity。
- final review round/generation/fingerprint/checks/coverage identity。
- artifact base/source branch/worktree/common Git identity。
- candidate tree OID。
- expected parent OID。
- 固定 author/committer name、email、UTC timestamp 和 message template version。
- operation version 和 pending timestamp。

source commit metadata 固定：

- author/committer name：`Coding Agent`。
- email：`coding-agent@localhost`。
- timestamp：`ObjectPending` 接受事务生成并持久化的 UTC 整秒；author date 与 committer date 都固定为同一 epoch-second 和 `+0000`，其 exact Git environment bytes 一并持久化。
- message：ASCII template `coding-agent: deliver task <task-id> attempt <attempt>` 加单个终止 LF；持久 template version 和 exact message bytes。
- parent：artifact base commit，且仅一个 parent。
- tree：candidate tree。

`ObjectPending` 后使用清理环境执行 `commit-tree <tree> -p <base>`，message 只从应用私有 exact byte input 提供，不经过 editor、template 或 cleanup。commit object 不更新 ref、真实 index 或 worktree；返回 OID 后必须用 object inspection 证明 tree、parent 和全部 metadata/message bytes 精确，再由 StoreWriter 写 `CommitPending` 和 expected source commit OID。相同 persisted metadata/input 重放 `commit-tree` 必须得到相同 OID。

### 8.2 `CommitPending`

`CommitPending` 是允许修改真实 index/source ref 的前置 durable intent，必须绑定：

- `ObjectPending` 的全部 immutable provenance。
- exact expected source commit OID。
- 已验证 object shape 的 transition version。

### 8.3 source ref/index side effect

持有 repository lease 时：

1. 重新认证 source identity、fixed lock reason、HEAD、fingerprint 和 config/attributes digest。
2. 证明 candidate OID 的 object type 精确为 `tree`，并证明 expected source commit shape 的 tree 精确等于 candidate。
3. 在真实 index 执行固定 `read-tree --reset <candidate>`，不使用 `-u`，不读取 live worktree。
4. 紧接着只执行固定、无路径输入的 `update-index --refresh -q`，仅刷新已 staged 条目的 stat cache；它不读取文件内容、不使用 filter/helper，也不改变 worktree。
5. 使用固定 `diff-index` predicate 证明真实 index 精确等于 candidate tree。
6. 再次验证 expected source commit object shape。
7. 使用 `update-ref <source-ref> <expected-source> <base>` 做 CAS 更新。
8. 验证 source ref/HEAD、commit shape、index tree 和 clean status。
9. StoreWriter 原子写 `Committed` 和 transition。

不运行 `git commit`，从而避免 commit hook/editor；`commit-tree` 仍必须显式禁用签名并使用清理环境。

repository lease 只串行本应用 mutation，不是跨进程 Git index lock。第 4 节所述最终外部-writer 窗口不能靠额外 predicate 或仅观察 `index.lock` 消除；若产品需要不丢弃任意非协作外部 staging，必须另行设计真实 index ownership 或原子 index-CAS，而不是把本阶段的 sampled drift detection 表述为全局排他。

### 8.4 source recovery

`ObjectPending` 恢复要求 source ref/index/worktree 仍为 approved pre-stage 状态；满足时按持久 metadata 重新执行幂等 `commit-tree`、验证 object shape，并推进 `CommitPending`。object 已存在或因上次命令刚写入不改变分类。

`CommitPending` 恢复只接受：

- source ref=`base`、真实 index/worktree 仍是 approved fingerprint：ref/index side effect 未开始，可继续。
- source ref=`base`、真实 index tree=`candidate tree` 且 worktree与 index 一致：stage 已完成，可继续 CAS `update-ref`。若进程在 `read-tree` 成功、stat cache refresh 前终止，纯观察无法可靠区分 zero-stat 假阳性与实际 drift，必须保守进入 `ReconciliationRequired`，绝不在 classifier 中刷新真实 index。
- source ref=`expected source commit`，commit shape 精确且 worktree/index clean：side effect 已完成，补写 `Committed`。

其他 ref、tree、index、worktree 或 evidence 组合一律 `ReconciliationRequired` 并 poison repository。恢复不 reset、clean 或猜测哪一方正确。

runtime recovery intent 不能由原始 OID、fingerprint 或 directory identity 字段公开构造。它只可从已认证的 source capability、已绑定的 candidate tree 和（如有）expected source commit capture，连同不进入 API/日志/错误的 opaque common/admin durable identity evidence；每次 fresh bind 都必须与当前 source capability 精确比较，比较失败不得构造 candidate 或执行 Git 命令。跨进程把 Store record 转为该 runtime intent 的受信 adapter 属于 Task 21 边界，Task 12 不宣称已完成该 adapter。

## 9. 目标 checkout

### 9.1 目标选择

P4-B 只合并到登记仓库路径当前 checkout 的 symbolic local branch：

- UI 显示并要求用户确认当前 branch 和 HEAD。
- 请求携带 `target_branch` 和 `expected_target_head`。
- 服务端要求请求值与 fresh observation 完全一致。
- detached HEAD、branch 不一致或用户切换分支均拒绝。
- P4-B 不替用户 checkout/switch 其他分支，也不在另一个 worktree 下更新已 checkout ref。

### 9.2 clean 条件

目标必须满足：

- common Git identity 与登记 repository 一致。
- root/admin capability 身份未变。
- symbolic branch 是合法 UTF-8 `refs/heads/*`，且不等于 source branch。
- `HEAD` 精确等于 expected target HEAD。
- porcelain v2 status 为空，包括 untracked files。
- 对 candidate merge 会写入或删除的每个相对路径及其父级做 capability-bound ignored-untracked collision 检查；任何已存在的 ignored 文件、目录或 symlink 冲突都拒绝。
- index 无 unmerged entries。
- 不存在 merge/rebase/cherry-pick/revert/bisect 等进行中的 Git operation。
- config/attributes 安全扫描通过。
- repository 未 poisoned。

dirty、ignored-path collision、detached、HEAD drift 或 operation in progress 都是零目标副作用拒绝。应用不得 stash、reset、clean、checkout 或覆盖用户文件。实际 merge 仍必须固定传入 `--no-overwrite-ignore`，作为 preflight 后外部创建 ignored 文件时的最后一道无覆盖保护。

## 10. merge preflight

### 10.1 持久 preflight operation

`POST merge/preflight` 是用户显式请求，但不更新 source/target ref、index 或文件。它创建一条 durable merge operation，状态转换为：

```text
PreflightPending -> PreflightReady
PreflightPending -> Conflict
PreflightPending -> Rejected
PreflightPending -> Stale
PreflightPending -> ReconciliationRequired

PreflightReady -> Accepted
PreflightReady -> Stale
PreflightReady -> Superseded
```

每个 preflight 绑定：

- eligibility evidence tuple。
- candidate source tree 和 preflight-only commit identity。
- target branch/HEAD。
- config/attributes digest。
- merge-base、candidate merge tree。
- bounded conflict path summary。
- canonical request hash。

同一 Task 最多一个 open preflight。创建新 preflight 时：

- 已有 `PreflightPending`：只有同一 command receipt 的重放返回 Existing；其他请求返回 `DELIVERY_OPERATION_IN_PROGRESS`。
- 已有 `PreflightReady`：在同一个 Store transaction 中以 version CAS 把旧 operation 写为 `Superseded`，再创建新的 `PreflightPending`。
- 已有 side-effect-active、`Merged` 或 `ReconciliationRequired`：不得创建新 operation。

POST merge fresh validation 发现 target/evidence/source stale 时，以 version CAS 把该 `PreflightReady` 写为 `Stale` 后返回 409。`Stale`/`Superseded` 不占 open-operation unique index。新 preflight 与用户确认并发时，只有一个 transaction 能赢得旧 version；不会出现两个 `Accepted` operation。

### 10.2 conflict 计算

source 尚未 `Committed` 时使用绑定 candidate tree 的临时 source commit object；source 已 `Committed` 时必须直接使用 persisted exact source commit。随后执行：

```text
git merge-tree --write-tree --messages --name-only -z <target> <source-candidate>
```

preflight-only commit 只为 `merge-tree` 提供 commit parent/tree 语义，使用固定无敏感 metadata；它不更新 source ref，也不是第 8 节最终 source commit。最终 source commit 只有在用户确认 merge 并持久 `ObjectPending` 后才产生。

preflight 和确认都必须检查 source 是否已经是 target HEAD 的 ancestor。若是，返回稳定 `SOURCE_ALREADY_IN_TARGET`，不把外部集成伪造成 P4-B 成功，也不进入 `MergePending`。

要求 Git >= 2.45。object database 中可能产生不可达 tree/commit object，但 target/source ref、真实 index 和文件不得变化。

结果：

- clean：持久 `PreflightReady` 和 candidate merge tree。
- conflict：持久 `Conflict` 和 bounded relative paths；目标必须再次验证仍 clean/HEAD unchanged。
- command/parse/identity 不确定：`ReconciliationRequired` + poison。

冲突路径最多 128 条，单路径 wire value 最多 4096 bytes，总 payload 最多 64 KiB。路径使用 `{encoding: utf8|base64url, value}`，不返回绝对路径或内容。

### 10.3 preflight 恢复

`PreflightPending` 不授权修改 source/target ref、真实 index 或文件。启动恢复重新观察 operation 绑定的 source/target/evidence/config：

- identity 全部相同：可幂等重建临时 tree/commit 并重跑 `merge-tree`。
- target HEAD、evidence 或 approved source fingerprint 已知变化：写 `Stale`，不 poison，也不修改 Git。
- command 只可能留下不可达 object 且 ref/index/worktree 均保持不变：按结果写 `PreflightReady|Conflict|Rejected`。
- ref/index/worktree 出现无法归因的变化或 identity 不可证明：`ReconciliationRequired` + poison。

## 11. merge operation 状态机

用户确认只允许把同一 `PreflightReady` operation 推进：

```text
PreflightReady
  -> Accepted
  -> MergePending
  -> Merged

Accepted + source 已 Committed，或 MergePending + 已知目标零副作用失败
  -> Failed

实际 merge 意外冲突
  -> AbortPending
  -> Conflict

任何未知 child outcome、漂移或无法证明的恢复
  -> ReconciliationRequired
```

source 尚未 committed 时，`Accepted` operation 先驱动第 8 节 source 状态机。`Accepted` 已持久固定 merge metadata、candidate tree 和两 parent identity，因此 source `Committed` 后可以幂等生成并验证 expected merge commit object；只有 exact expected merge OID 已持久后才进入 `MergePending`。

source `ObjectPending|CommitPending` 的已知未应用错误不得把 merge operation 写为 `Failed`。它保留 `Accepted + pending source`、稳定 retryable error 和 bounded backoff，由同一 actor/startup 继续恢复；只有 source `Committed` 后发生的已知目标零副作用拒绝才允许 `Failed`。Store CHECK/transition validator 禁止 `merge=Failed + source=ObjectPending|CommitPending`。

### 11.1 确认时重新验证

POST merge 必须重新验证：

- operation 仍为 `PreflightReady`。
- task/evidence/artifact 未变。
- source 仍匹配 approved fingerprint，或已有精确 `Committed` source。
- target branch/HEAD/clean/config digest 未变。
- candidate merge tree 重新计算一致。
- source commit 不是 target HEAD 的 ancestor。
- candidate write-set 没有 ignored-untracked collision。
- 用户提交的 expected generation/fingerprint/target branch/HEAD 与 operation 一致。

任何 stale 值先把仍为 `PreflightReady` 的 operation 原子终结为 `Stale`，再返回稳定 409；evidence、target、source 分别使用既有 typed stale code，operation 已 `Stale|Superseded` 或 version 不匹配使用 `DELIVERY_PREFLIGHT_STALE`。不能把旧 modal 的确认用于新状态。

### 11.2 `MergePending`

实际 merge 前持久化：

- exact source commit。
- exact target branch/old HEAD。
- merge-base 和 candidate merge tree。
- 固定 merge author/committer/timestamp/message version。
- exact expected merge commit OID 和完整 commit shape。
- operation version。

固定策略：

- `--no-ff`。
- first parent=expected target HEAD。
- second parent=source commit。
- message=`coding-agent: merge task <task-id> attempt <attempt>`。
- message bytes 固定为 UTF-8 ASCII template 加单个终止 LF；author/committer timestamp 固定为 `Accepted` transaction 持久化的 UTC 秒和 `+0000`。
- hooks、签名、editor、message cleanup、autostash、rerere、custom merge driver 全禁用或固定。

进入 `MergePending` 前，以 persisted candidate tree、target HEAD 第一 parent、source commit 第二 parent 和完整固定 metadata 幂等执行 `commit-tree`，验证 object shape，并持久 exact OID。`Accepted` 是该不可达 object side effect 的 durable intent；崩溃后使用同一 metadata 重放必须得到同一 OID。target ref/index/worktree 在 `MergePending` commit 前不得改变。

### 11.3 实际 merge

持有 repository lease：

1. 重新认证 source committed state、worktree present/clean 和 fixed lock reason。
2. 重新认证 target exact clean/branch/HEAD/config。
3. 再次确认 source 不是 target HEAD 的 ancestor，并检查 ignored-untracked collision。
4. 执行固定 typed merge command，参数只含 validated source OID、固定策略/安全开关和固定 message；环境使用与 expected object 完全相同的 author/committer/timestamp。
5. 验证 target HEAD 精确等于 persisted expected merge commit OID。
6. 验证 tree=candidate merge tree、parents 顺序、metadata/message bytes、target clean、无 MERGE_HEAD。
7. StoreWriter 在同一 transaction 原子写 `Merged` transition，并首次创建 `task_artifact_dispositions(worktree=RetainedLocked, branch=Retained)`、初始 version/journal；不能留下 `Merged` 但缺 disposition 的 crash 窗口。

用户请求断开不取消 durable accepted operation。UI 通过 operation GET polling 获得结果。

## 12. 冲突与 abort

### 12.1 正常冲突

preflight 已发现冲突时不执行实际 merge：

- operation=`Conflict`。
- target branch/HEAD/index/worktree 保持不变。
- UI 显示 bounded conflict paths。
- 用户只能在外部修复/推进目标分支后创建新的 preflight operation。
- P4-B 不自动修改 source 或 target 文件。

### 12.2 意外冲突

若 preflight clean 后实际 merge 仍报告 conflict，只能在以下全部成立时自动 abort：

- target symbolic branch/old HEAD 与 operation 一致。
- `MERGE_HEAD` 精确等于 source commit。
- merge/index stage entries 与本次 child outcome 一致。
- 没有额外 untracked 或无法归因的外部修改证据。
- process outcome 已知为 conflict，不是 wait/channel unknown。
- `MERGE_AUTOSTASH` 明确不存在；存在、不可观察或 identity 不确定都禁止自动 abort。

已知 conflict child outcome 后，先通过 StoreWriter 持久 `AbortPending`，同时绑定 expected old HEAD、source commit、`MERGE_HEAD`、index stages、worktree conflict observation digest、`MERGE_AUTOSTASH=absent` proof 和 child receipt；再执行 `merge --abort`。abort 前每次 retry 都必须重新证明 `MERGE_AUTOSTASH` 仍不存在，否则直接 `ReconciliationRequired` + poison。abort 后必须证明：

- target HEAD=expected old HEAD。
- target clean。
- 无 MERGE_HEAD/merge state。
- 无 `MERGE_AUTOSTASH`。
- source ref 未变。

证明成功才写 `Conflict`。任何不一致都不运行 reset/clean；写 `ReconciliationRequired` 并 poison。

## 13. merge 崩溃恢复

恢复必须先按 durable state 分流，不能把 `MergePending` 与 `AbortPending` 共用一个含糊分类。

`MergePending` 的观察分类固定：

1. target=expected old HEAD、clean、无 merge state：merge 未应用，可重试 exact merge。
2. target=精确 persisted expected merge commit、tree/parents/metadata/message 正确、clean：merge 已应用，补写 `Merged`。
3. target=old HEAD、存在 conflict state，但没有 durable known-conflict child receipt/`AbortPending`：child outcome 无法证明，直接 `ReconciliationRequired` + poison，不自动 abort。
4. target/ref/index/worktree/config 出现其他组合：`ReconciliationRequired` + poison。

`AbortPending` 的观察分类固定：

1. target=old HEAD、仍存在与 durable conflict observation digest 精确相同的本次 conflict state，且 `MERGE_AUTOSTASH` 仍明确不存在：abort 未应用，可重试 exact abort。
2. target=old HEAD、clean、无任何 merge state：abort 已应用，补写 `Conflict`，不得再次执行 merge。
3. target/ref/index/worktree/config 出现其他组合：`ReconciliationRequired` + poison。

Store outcome commit 回执未知时，DeliveryManager 保持 lease，先查询 exact `(operation_id, version, transition)` receipt。channel close 不能解释为“未执行”。

成功 `Merged` 后目标分支被外部 reset 不改写历史 operation；cleanup 时重新做 ancestry proof，失败则拒绝 branch delete。

## 14. Git config、attributes 与命令安全

### 14.1 配置边界

每个阶段读取 capability-bound local config 原文件并计算 digest。拒绝：

- `include.*` / `includeIf.*`。
- `filter.*.clean/smudge/process`。
- `diff.*.command/textconv`。
- custom `merge.*.driver`。
- 任意 `branch.*.mergeOptions`；不能允许 repository config 注入 `--squash`、`--no-commit`、strategy、signing、autostash 或其他 merge 参数。
- 无法由 exact CLI/config override 中和的 `merge.verifySignatures`；实际命令仍固定 `--no-verify-signatures`，避免执行 GPG/SSH verification program。
- 非空 hooksPath 指向外部位置。
- autostash、rerere 或 signing 强制配置无法被固定 override 的组合。

操作命令设置：

- 复用 P4-A `GitCommandBinding` 的完整固定前缀与 capability revalidation，包括 `--no-replace-objects`、`--no-lazy-fetch`、`core.fsmonitor=false`、`core.untrackedCache=false`、`submodule.recurse=false`、empty external excludes/attributes 和 `diff.external=`；不能为 P4-B 另造较弱的命令路径。
- `GIT_CONFIG_NOSYSTEM=1`。
- Unix 的 `GIT_CONFIG_SYSTEM`/`GIT_CONFIG_GLOBAL` 绑定到应用私有空配置的 retained FD：`0600` exclusive create 后立即 unlink，防止后续 namespace 替换或按路径重开。该边界不防御同一 UID 的恶意方在 unlink 前已取得可写 FD；若需该保证，必须使用 OS-specific anonymous FD 或隔离。Windows 使用固定 `NUL` 空配置端点。
- `core.hooksPath` 固定为 Unix `/dev/null` 或 Windows `NUL`，不使用可写的应用目录。
- `commit.gpgSign=false`、`merge.gpgSign=false`。
- `merge.verifySignatures=false`。
- `merge.autoStash=false`、`rerere.enabled=false`。
- editor/pager/askpass/credential helper 相关环境清空。
- 清空 `GIT_CONFIG_COUNT`、`GIT_CONFIG_KEY_*`、`GIT_CONFIG_VALUE_*` 和其他 config-injection 环境；只注入应用构造的 allowlisted `-c key=value`。

### 14.2 attributes

对候选变更和 merge 涉及路径执行 bounded attributes 检查。拒绝：

- `filter` attribute。
- custom `merge` attribute/driver。
- submodule/gitlink。
- 需要外部程序的 diff/working-tree encoding 组合。

candidate tree 不通过 `add` 读取 live worktree；它只把已审计 snapshot 的 exact bytes 经 `hash-object --no-filters` 和 typed index-info 写入临时 index。因此 attributes 重验仍是必要认证步骤，但不是该构造路径防止 filter/helper 执行的唯一防线。

### 14.3 命令构造

允许的高层 typed 动作：

- rev/object/symbolic-ref/status/config/attribute 观察。
- 临时 index `read-tree`、固定 `hash-object -w --no-filters --stdin`、typed `update-index --add --replace -z --index-info` 和 `write-tree`；真实 source index 的固定 `read-tree --reset <candidate>`、`update-index --refresh -q` stat-cache refresh 与 `diff-index` exact predicate。
- `commit-tree` 和 CAS `update-ref`。
- target preflight 的 `merge-base --all <target> <source>`（输出必须恰好一个 typed base）与 `merge-tree --write-tree --messages --name-only -z <target> <source>`。
- clean merge result 的固定 `diff-tree --no-commit-id --name-only -r -z --no-renames --no-ext-diff <target> <merged-tree>` write-set scan，以及固定 `ls-files --others --ignored --exclude-standard --directory -z --` ignored-untracked scan。
- 固定 `merge --no-ff --strategy=ort --no-edit --no-verify --no-verify-signatures --no-gpg-sign --no-autostash --no-rerere-autoupdate --no-overwrite-ignore --no-log --no-stat --cleanup=verbatim -m <fixed-message> -- <source-oid>`；`--no-log` 防止 `merge.log` 改写 expected message，`--no-stat` 固定输出边界；启动 probe 必须证明当前 Git 支持完整 option set。
- exact `merge --abort`。
- `merge-base --is-ancestor`。
- exact `worktree unlock/remove`。
- 单个 `update-ref --stdin` transaction 内 verify target ref/head 并 CAS delete source ref。

禁止：

- shell、任意 argv、任意 cwd、任意 program。
- `reset --hard`、`clean`、stash。
- force worktree remove、force branch delete。
- 用户提供 commit message/author/config/pathspec。
- 输出原始环境、绝对路径、prompt、diff 内容或 Git stderr。

每个 child 使用固定 deadline、stdout/stderr bounds 和 process-tree cleanup proof。timeout 后 outcome 未知时进入 reconciliation，不盲目重试。

## 15. post-merge Git 现场清理

### 15.1 默认状态

merge 成功后：

- source worktree 默认保留且保持 locked。
- source branch 默认保留。
- review evidence、Task、events、delivery rows 永久保留；P4-B 不删除历史数据。

UI 提供两个独立动作和两次确认：

1. Remove worktree。
2. Delete source branch。

### 15.2 worktree disposition

```text
RetainedLocked
  -> RetainedUnlocked
  -> Removed

任一无法证明的事实
  -> ReconciliationRequired
```

对应的 `remove_worktree` cleanup operation 状态固定为：

```text
UnlockPending
  -> UnlockedPendingRemove
  -> RemovePending
  -> Completed

已知零副作用失败 -> Failed
未知、partial 或 identity mismatch -> ReconciliationRequired
```

disposition 只记录已经证明的 Git 事实，不承载 pending intent。operation 与 disposition 的组合固定为：`UnlockPending + RetainedLocked`、`UnlockedPendingRemove + RetainedUnlocked`、`RemovePending + RetainedUnlocked`、`Completed + Removed`。每次事实变化和两侧 version/journal 在同一 Store transaction 提交。`UnlockedPendingRemove` 是已经接受的同一个用户操作，actor 或 startup 必须自动继续到 `RemovePending`，不能把它误当成需要第二次用户确认的空闲状态。

允许 remove 的条件：

- merge operation=`Merged`。
- source=`Committed`。
- worktree identity 与 artifact 精确匹配。
- worktree clean 且 HEAD/source ref=source commit。
- TaskManager 无 active ownership。
- 无存活/未知 process tree。
- 无 active merge/cleanup。
- repository 未 poisoned。

worktree 由应用以固定 lock reason 创建。remove operation 先持久 `UnlockPending + RetainedLocked`，精确 unlock；验证成功后持久 `UnlockedPendingRemove + RetainedUnlocked`，再持久 `RemovePending + RetainedUnlocked`，最后执行不带 `--force` 的 exact remove。

unlock 的已知零副作用失败写 `Failed + RetainedLocked`；unlock 已验证成功后写 `UnlockedPendingRemove + RetainedUnlocked`。remove 的已知零副作用失败写 `Failed + RetainedUnlocked`，允许用户在 fresh validation 后用新的 receipt 重试。`COMMAND_TIMED_OUT` 只有在 process-tree cleanup 后的 fresh exact observation 另行证明 Git 事实未改变时，才可作为 typed known-not-applied diagnostic；否则 timeout 属于 unknown outcome，写两侧 `ReconciliationRequired` 并 poison。

### 15.3 branch disposition

```text
Retained
  -> Deleted

无法证明的事实
  -> ReconciliationRequired
```

对应的 `delete_branch` cleanup operation 状态固定为：

```text
DeletePending
  -> Completed

已知零副作用失败 -> Failed
未知或 ref identity mismatch -> ReconciliationRequired
```

branch disposition 同样只记录事实。operation 与 branch disposition 的组合为 `DeletePending + Retained`、`Completed + Deleted`；两侧 version/journal 原子提交。已知未应用失败保持 `Retained + Failed`，新请求必须使用新的 command receipt。原子 ref transaction timeout 后，只有 fresh exact observation 证明 source ref 仍为 expected OID、因而 transaction 未删除 source 时，`COMMAND_TIMED_OUT` 才是 typed known-not-applied；否则 timeout、unknown 或 mismatch 使两侧进入 `ReconciliationRequired` 并 poison。

允许 delete 的条件：

- worktree disposition=`Removed`。
- source ref 精确指向 persisted source commit。
- source commit 是当前 target branch HEAD 的 ancestor。
- target branch/merge operation identity 一致。
- source branch 未被其他 worktree checkout。
- 无 active operation/process tree。

`DeletePending` 必须绑定 authenticated common Git identity、exact target ref、fresh target HEAD、source ref 和 expected source OID。删除不是仅对 source ref 做单独 CAS，而是使用一个原子 ref transaction：

```text
git update-ref --stdin
  start
  verify refs/heads/<target> <expected-target-head>
  delete refs/heads/<source> <expected-source-oid>
  prepare
  commit
```

fresh target HEAD 必须已证明包含 source commit。`verify target + delete source` 在同一 ref transaction 中提交，任一 CAS 不匹配都零删除并重新观察；不使用 `branch -D`。若 target 合法前进且仍包含 source，必须先通过 StoreWriter 持久新的 `DeletePending` version/expected target HEAD，再尝试新 transaction；不能沿用旧 ancestry proof。

### 15.4 cleanup 恢复

恢复只从 active cleanup operation 驱动，并核对 disposition/version 映射；不得只看 disposition 猜测用户是否接受过 cleanup。

`UnlockPending + RetainedLocked`：

- exact locked/present 且全部 identity 一致：unlock 未应用，可重试。
- exact unlocked/present 且 identity 一致：unlock 已应用，补写 `UnlockedPendingRemove + RetainedUnlocked`。
- path/admin absent、partial、identity mismatch 或不可观察：`ReconciliationRequired` + poison。

`UnlockedPendingRemove + RetainedUnlocked`：

- exact unlocked/present 且 clean/identity 一致：持久 `RemovePending` 后继续。
- relocked、absent、partial、dirty、identity mismatch 或不可观察：`ReconciliationRequired` + poison；不得再次 unlock 或猜测已 remove。

`RemovePending + RetainedUnlocked`：

- exact unlocked/present 且 clean/identity 一致：remove 未应用，可重试 exact non-force remove。
- path 和 admin 都 absent，source ref 仍为 expected OID 且 common identity 一致：remove 已应用，补写 `Completed + Removed`。
- dirty 且 identity 仍精确：remove 已知未应用，写 `Failed` 并保持 `RetainedUnlocked`，不覆盖用户文件。
- locked、partial、identity mismatch 或不可观察：`ReconciliationRequired` + poison。

`DeletePending + Retained`：

- 每次 retry 前重新认证 common identity、persisted target ref 和 fresh target HEAD，并重新证明 source 是该 HEAD 的 ancestor；target 合法前进时先持久新 operation version/HEAD。
- source ref=expected OID：只允许执行上述 `verify target + delete source` 原子 transaction。
- source ref absent：只有 fresh target ancestry 和 expected source object shape 仍可证明时才补写 `Completed + Deleted`。
- source ref 仍为 expected OID，但 target 缺失或 fresh HEAD 不再包含 source：delete 已知未应用，写 `Failed + Retained` 和稳定 `SOURCE_BRANCH_NOT_MERGED`，绝不删除。
- source ref 指向其他 OID、source 已 absent 且 target ancestry 也无法证明、identity mismatch 或不可观察：`ReconciliationRequired` + poison，绝不删除或重建 ref。

P4-B 不自动回收 dangling object；Git 自身 GC 不属于本操作。

## 16. SQLite v5

新增 migration：

```text
0005_controlled_delivery.sql
```

### 16.1 `task_delivery_sources`

至少包含：

- `task_id` PK/FK。
- `repository_id`、`attempt`。
- evidence round/generation/fingerprint/checks/coverage identity。
- artifact base/source branch/worktree/common Git identity。
- candidate tree、nullable expected source commit。
- fixed commit metadata/template version。
- `state = object_pending|commit_pending|committed|reconciliation_required`。
- failure code、version、created/updated timestamps。

identity/provenance 字段创建后不可更新；只允许单向 state/version 转换。

### 16.2 `task_merge_operations`

至少包含：

- operation ID PK。
- task/repository/attempt identity、candidate source identity；delivery source/commit reference 在 `Accepted` 前可空、绑定后不可变。
- preflight command receipt reference 和 nullable accept command receipt reference。
- target branch/expected old HEAD。
- evidence tuple/config digest。
- merge-base/candidate tree/merge commit。
- fixed merge metadata/template version；`MergePending` 起 expected merge commit OID 非空。
- state=`preflight_pending|preflight_ready|accepted|merge_pending|merged|abort_pending|conflict|rejected|stale|superseded|failed|reconciliation_required`、failure code、version、timestamps。

DB partial unique index 的状态集合必须精确写死：

- 同一 Task 最多一个 open/side-effect-active operation：`preflight_pending|preflight_ready|accepted|merge_pending|abort_pending`。
- 同一 Task 最多一个 `merged` operation。

创建新 preflight 的 transaction 必须先以 version CAS 把旧 `preflight_ready` 变为 `superseded`，并在同一 transaction 拒绝已有 `merged` 或 `reconciliation_required`。`reconciliation_required` 不占上述 partial unique index，但由 trigger/transition query 永久阻断新 mutation，直到未来明确的人工修复能力；P4-B 不提供清除入口。

### 16.3 `task_merge_conflicts`

- operation ID FK。
- ordinal。
- path encoding/value。
- `PRIMARY KEY(operation_id, ordinal)`。
- path/count/total bounds 由 Store 输入 validator 和 DB length CHECK 双重保护。

### 16.4 `task_artifact_dispositions`

- task/repository/attempt identity。
- merged operation/source commit reference。
- worktree disposition/version：`retained_locked|retained_unlocked|removed|reconciliation_required`，只记录已证明事实。
- branch disposition/version：`retained|deleted|reconciliation_required`，只记录已证明事实。
- failure code/timestamps。

### 16.5 `task_cleanup_operations`

- operation ID PK。
- `remove_worktree|delete_branch`。
- origin command receipt reference。
- expected path/admin/common identity/source ref/source OID/disposition version；branch delete 另含 exact target ref 和本次持久的 expected target HEAD。
- `remove_worktree` state=`unlock_pending|unlocked_pending_remove|remove_pending|completed|failed|reconciliation_required`。
- `delete_branch` state=`delete_pending|completed|failed|reconciliation_required`；kind/state 组合由 CHECK/trigger 限定。
- failure code、version、timestamps。
- 同一 artifact/disposition 最多一个 active operation；active 精确为 `unlock_pending|unlocked_pending_remove|remove_pending|delete_pending`。

`delete_branch` 的 mutable current `expected target HEAD` 不能单独承担 reply-lost 重放证据。另建 immutable child journal `task_cleanup_target_head_observations`：

- `(cleanup operation ID, operation version)` 主键，保存该版本 exact target HEAD 和与 cleanup transition 相同的 timestamp。
- `delete_branch` 从 v1 到 current version 每版恰有一行；v1 等于 immutable origin target HEAD，current 等于 current expected target HEAD。
- `DeletePending -> DeletePending` refresh 必须改变 HEAD；terminal transition 必须保留上一版 HEAD；`remove_worktree` 不得有该 child row。
- child row 与对应 cleanup transition 使用 deferred FK，并由 cleanup current-row journal trigger 在同一事务自动写入；禁止 replace/update/delete。
- Store 读取时审计连续版本、OID algorithm、端点、timestamp、orphan/missing/extra/mismatch 后，才允许按历史版本判定 exact refresh replay；不得用 origin 或 current HEAD 猜测中间版本。

### 16.6 `task_delivery_command_receipts`

每个用户 POST 有独立 immutable command receipt；merge preflight 与 merge accept 不能复用 operation row 上的一组 idempotency 字段。至少包含：

- `client_request_id` PK，canonical UUID text。
- `command_kind = preflight|accept_merge|remove_worktree|delete_branch`。
- task/repository/attempt identity。
- canonical request SHA-256。
- operation kind/ID、accepted operation version/state 和 response discriminator。
- created timestamp。

receipt 与对应 operation 创建或 state transition 必须在同一个 Store transaction 中提交。相同 client ID + 相同 kind/hash 返回该 receipt 指向的 Existing operation；相同 ID 的任意 kind/hash 差异返回 `IDEMPOTENCY_CONFLICT`。`UNIQUE(command_kind, operation_id)` 防止同一 operation 获得两个成功 accept/cleanup receipt；preflight operation 也恰有一个 origin receipt。

### 16.7 `task_delivery_operation_transitions`

immutable transition journal：

- global transition ID。
- entity kind/ID：`delivery_source|merge_operation|cleanup_operation|worktree_disposition|branch_disposition`。
- entity version。
- from/to state。
- stable failure code/null。
- timestamp。
- `UNIQUE(entity_kind, entity_id, entity_version)`。

current row transition 和 journal insert 必须同事务提交。

### 16.8 兼容性

- v1-v4 数据迁移到 v5 不创建虚假 source/merge/cleanup rows。
- future schema version 继续拒绝。
- 重复 migrate 幂等。
- TaskStatus/readiness/review/artifact/event rows不改写。
- 既有 11 种 task event、`Task.last_event_id` 和 SSE cursor 不因 delivery operation 变化。

## 17. 幂等与线性化

所有 POST 携带全局唯一 UUID `client_request_id`，每次 preflight、merge accept、remove worktree 和 delete branch 都产生自己的 command receipt。

- request canonicalization 覆盖 task、operation、expected evidence、target branch/HEAD 和 action。
- 相同 client ID + 相同 hash 返回 Existing。
- 相同 client ID + 不同 hash 返回 `IDEMPOTENCY_CONFLICT`。
- 同一 merge operation 的 preflight receipt 与 accept receipt 分开持久化；重放任一阶段都返回它原来绑定的 operation，而不创建第二个 side effect。
- HTTP disconnect 不回滚 durable acceptance。
- 用户双击、重试和重启后重发不能重复 commit/merge/delete。

线性化点：

- preflight：command receipt 与 operation `PreflightPending` 同事务 commit。
- merge 用户接受：独立 command receipt 与 `PreflightReady -> Accepted` 同事务 commit。
- source object intent：source `ObjectPending` commit。
- source ref/index intent：source `CommitPending` commit。
- source success：source `Committed` commit，但 Git source ref side effect 在它之前已被精确验证。
- merge intent：operation `MergePending` commit。
- merge success：operation `Merged` commit，但目标 Git side effect 在它之前已被精确验证。
- cleanup acceptance receipt、intent/outcome 同理。

“Git 已成功但 Store outcome reply 丢失”必须通过 pending row 和真实 Git observation 恢复，不能再次盲目执行。

## 18. 锁顺序与并发

固定顺序：

```text
DeliveryManager actor decision
  -> RepositoryControlCoordinator non-blocking lease
  -> StoreWriter pending transition
  -> typed runtime Git side effect
  -> StoreWriter observed-outcome transition
  -> verified lease release
```

约束：

- API handler 不持有 lease；只提交 actor request/等待 durable acceptance。
- StoreWriter 不获取 repository lease 或等待 DeliveryManager。
- runtime callback 不调用 StoreWriter/TaskManager/Scheduler。
- SQLite transaction 不跨 Git command。
- 同 repository merge/source/cleanup 与 P4-A worktree reservation/recovery 串行。
- 不同 repository 可以并行，但仍受内部全局 DeliveryManager Git-operation cap；P4-B 固定为 2，不新增配置项、API 或 Web 设置。
- 已运行的其他 task worktree 不自动停止；其后续 merge 必须面对新的 target HEAD/preflight。
- repository busy 返回稳定 retryable error，不在 lease 内排队等待。
- poison 只影响对应 authenticated common Git identity；无法界定 identity 时冻结 delivery mutation，不伪装为普通 busy。

## 19. 启动恢复与关闭

### 19.1 启动

本节只在完整 P4-A 冷启动顺序中插入 delivery reconciliation，不替换、删除或放宽 P4-A 第 16.3 节的任何阶段：

1. 在 single-instance lock 内加载并验证 runtime/provider config 与私有路径，尚不打开 SQLite。
2. 先证明上一实例全部 process-liveness sentinel 可独占；未证明时不得打开/迁移 SQLite 或开放 Web。
3. 打开 SQLite，验证连续 migration history 并完成 v1-v5 migration。
4. 初始化 RepositoryControlCoordinator 和 authenticated aliases。
5. 在一个一致 read 中按 task/attempt join artifact、delivery source、merge 和 disposition；存在合法 P4-B row 的 artifact 标记为 delivery-owned。delivery row 只能引用 `Ready` artifact；`Reserved/Inconsistent` + delivery ownership 或任何缺失/矛盾 join 使 startup fail closed。
6. 使用 startup-only direct Store 操作，按 P4-A 原规则幂等校正所有非 delivery-owned `Reserved` artifact；delivery-owned artifact 不进入要求 `branch=base + worktree present` 的 P4-A ready observer。
7. 执行 P4-A 原有单个 `recover_after_restart BEGIN IMMEDIATE`：验证 intent/terminal tuple，终态化 Running stop intent，把其余 Running 写为 `Interrupted + APP_RESTARTED`，保持 Queued，并得到 committed event high watermark。
8. 以该 high watermark 初始化并启动 EventDispatcher，随后启动唯一生产 StoreWriter；外部 mutation gate、DeliveryManager、TaskManager 和 Scheduler 仍关闭。
9. 通过 StoreWriter，按 common Git identity 和 operation creation order 恢复全部 delivery-owned source、merge 和 active cleanup operation。每条 operation 必须收敛到可继续、已完成、已知失败或 `ReconciliationRequired + poison`；不能回退到 P4-A observer 猜测。
10. 从 fresh Running=0、Queued、durable stop intent、storage sample 和 delivery recovery 结果初始化并启动 DeliveryManager、TaskManager、Scheduler，完成 P4-A Bootstrap 因果校验。
11. 完成 exact bootstrap 后才开放 Web Ready/重扫。

启动恢复只校正已经 durable accepted 的用户操作，不创建新 preflight/merge/cleanup。

### 19.2 关闭

- mutation gate 先拒绝新 delivery mutation。
- DeliveryManager 停止接受新 operation。
- 已 durable accepted 的 operation 完成当前可证明阶段或进入 pending recovery state。
- 等待所有 Git child process tree。
- child outcome unknown 时不得释放 repository/single-instance cleanup ownership。
- 复用 P4-A shutdown failsafe：无法证明进程树退出时关闭 HTTP、冻结调度并继续持有 primary lock。

## 20. REST 与 OpenAPI

### 20.1 查询

```text
GET /api/tasks/{task_id}/delivery
GET /api/delivery-operations/{operation_id}
```

delivery response 包含：

- eligibility 与稳定 reason code。
- evidence summary（generation/fingerprint，绝不含 prompt/diff）。
- source state/commit。
- latest merge operation。
- worktree/branch disposition。
- allowed actions。

### 20.2 preflight

```text
POST /api/tasks/{task_id}/merge/preflight
```

request：

- `client_request_id`
- `target_branch`
- `expected_target_head`

response：

- operation ID/version/state。
- exact review generation/fingerprint。
- source candidate identity。
- target branch/HEAD。
- conflict summary。
- confirmation fields。

新建返回 201；同 idempotent request 返回 200 Existing。

### 20.3 用户确认 merge

```text
POST /api/tasks/{task_id}/merge
```

request：

- `client_request_id`
- `preflight_operation_id`
- `expected_operation_version`
- `expected_review_generation`
- `expected_workspace_fingerprint`
- `target_branch`
- `expected_target_head`

durable accepted 返回 202；existing 返回 200。handler 不等待完整 Git merge。

### 20.4 cleanup

```text
POST /api/tasks/{task_id}/cleanup/worktree
POST /api/tasks/{task_id}/cleanup/branch
```

分别携带 client request ID、expected disposition version、expected source/merge identity。两个动作不能合并为一个按钮或一个隐式级联请求。

### 20.5 安全

全部 POST 必须：

- authenticated session。
- exact loopback Origin。
- CSRF。
- mutation gate Ready。
- bounded JSON，拒绝 unknown fields。
- durable idempotency。

GET 仍需 session。接口不接收任意路径、refspec、argv、message 或 author。

## 21. 稳定错误

至少定义：

- `TASK_NOT_MERGE_ELIGIBLE`
- `DELIVERY_EVIDENCE_STALE`
- `DELIVERY_SOURCE_CHANGED`
- `DELIVERY_SOURCE_INCONSISTENT`
- `DELIVERY_PREFLIGHT_STALE`
- `DELIVERY_OPERATION_IN_PROGRESS`
- `TARGET_BRANCH_DETACHED`
- `TARGET_BRANCH_MISMATCH`
- `TARGET_HEAD_CHANGED`
- `TARGET_WORKTREE_DIRTY`
- `TARGET_IGNORED_PATH_COLLISION`
- `TARGET_GIT_OPERATION_IN_PROGRESS`
- `UNSAFE_GIT_CONFIGURATION`
- `UNSUPPORTED_GIT_ATTRIBUTES`
- `MERGE_CONFLICT`
- `SOURCE_ALREADY_IN_TARGET`
- `DELIVERY_RECONCILIATION_REQUIRED`
- `ARTIFACT_CLEANUP_NOT_ALLOWED`
- `ARTIFACT_PROCESS_STILL_ACTIVE`
- `WORKTREE_IDENTITY_MISMATCH`
- `SOURCE_BRANCH_NOT_MERGED`
- `IDEMPOTENCY_CONFLICT`

复用：

- `REPOSITORY_CONTROL_BUSY`
- `REPOSITORY_CONTROL_POISONED`
- `COMMAND_TIMED_OUT`
- `PROCESS_TREE_CLEANUP_FAILED`
- shutdown/degraded mutation errors。

message 固定、短且脱敏。不得返回绝对路径、prompt、diff、Git stderr、环境、config value 或冲突文件内容。

## 22. Web UX

Task Workspace 增加独立 Delivery panel：

1. 资格不满足时显示稳定原因，不显示可点击 Merge。
2. 点击 Preflight 后展示：
   - target branch/HEAD；
   - review generation/fingerprint 短摘要；
   - source state；
   - clean/conflict 结果。
3. Merge 使用二次确认，确认内容逐字段来自 preflight response。
4. operation accepted 后按 operation ID/version polling；pending 时禁用重复动作。
5. conflict 显示 bounded relative paths，提供“重新预检”，不提供自动修改。
6. merge 成功后先显示“保留 worktree/branch”。
7. Remove worktree 和 Delete branch 是两个按钮、两个 modal；branch 按钮只有 worktree Removed 且 fresh ancestry proof 可用时启用。
8. 页面刷新/重连从 GET delivery/operation 重建，不从本地按钮状态推断。

polling 使用有界退避，例如 500ms 起、最多 2s；进入 terminal state 后停止。P4-B 不修改 SSE task event union。

## 23. TDD 验收矩阵

### 23.1 domain/typed state

- eligibility 全矩阵。
- source/merge/cleanup 单向 state transition。
- OID 40/64、branch、fingerprint、path/conflict bounds。
- request canonicalization/idempotency conflict。
- preflight/accept/两个 cleanup POST 各自独立 receipt；跨 action 复用 UUID 冲突且 reply lost 可重放。
- exact serde、unknown field rejection。

### 23.2 migration/store

- 空库 v5。
- v1/v2/v3/v4 -> v5。
- 重复 migrate。
- future version 拒绝。
- FK/CHECK/partial unique/immutable identity。
- current transition + journal 原子性。
- writer 每一步 fault injection、reply lost、busy/rollback/unknown。
- 同 Task active merge、同 disposition active cleanup 竞争。
- stale/superseded preflight 释放 open-operation uniqueness；new-preflight 与 accept 的 version CAS 竞态只有一个胜者。
- `merged`/`reconciliation_required` 在同一创建 transaction 中阻断新 preflight，不能只靠应用层快照。
- 既有 Task/readiness/review/artifact/event 不变。

### 23.3 source runtime

- dirty reviewed source 可安全 reopen；P4-A `open_ready` 仍拒绝 dirty。
- source branch/path/admin/common Git/fixed lock reason mismatch 拒绝。
- approved fingerprint stale 拒绝且零 ref/index side effect。
- temp-index tree 从no-follow、identity-bound snapshot的exact bytes构造并与批准内容绑定；filter/helper对该构造路径零执行。
- real index 只经固定`read-tree --reset <candidate>`写入，随后`diff-index`必须证明其等于candidate tree；candidate先有`tree` type proof。
- deterministic commit-tree/CAS ref。
- hook/signing/filter/custom driver 零执行。
- source commit 每个 crash point 恢复。
- source pending 的已知未应用错误保持 `Accepted + pending` 可恢复，DB 拒绝 `Failed merge + pending source`。
- restart 时 `Committed` source 不被 P4-A reconciler 误判为 branch mismatch/inconsistent。

### 23.4 target/preflight/runtime merge

- detached、dirty、HEAD drift、branch mismatch、ongoing Git operation 拒绝。
- ignored-untracked collision 在 preflight 和 actual merge 都零覆盖拒绝；`--no-overwrite-ignore` 固定存在。
- 任意 `branch.*.mergeOptions` 拒绝，不能注入 squash/no-commit/strategy/signing/autostash；恶意 `merge.verifySignatures=true` 仍由 `--no-verify-signatures` 中和且不启动验证程序。
- clean preflight 目标 ref/index/files 不变。
- conflict preflight 目标 byte-for-byte 不变。
- no-ff merge tree、双 parents、固定 metadata。
- expected merge commit object/OID 可确定重放，actual HEAD 必须精确等于该 OID。
- source 已是 target ancestor 时稳定拒绝，不报告 P4-B merge 成功。
- 用户确认前 source/target ref 不变。
- 外部 target advance 返回 stale，不自动 merge。

### 23.5 conflict/recovery

- preflight conflict bounded path encoding。
- 意外 conflict exact abort；`MERGE_AUTOSTASH` 存在/不可观察时绝不 abort，并进入 reconciliation + poison。
- abort 无法证明 -> poison，不 reset/clean。
- merge child outcome unknown 的三种可证明分类和所有其他 poison。
- Store outcome reply 丢失后只补写，不重复 merge。
- restart 对 pending operations 收敛。
- `MergePending` 无 durable conflict receipt 时不自动 abort；`AbortPending` 按持久 conflict digest 恢复。

### 23.6 coordinator/shutdown

- 同 repo 双 merge、merge vs reservation、cleanup vs admission 串行。
- busy 不排队持有其他 permits。
- 不同 repo bounded 并行。
- poison 隔离。
- 无锁反转。
- shutdown 等待 Git process tree；未知进程继续持有 primary lock。

### 23.7 cleanup

- 未 merge/未审查/失败 attempt 全部拒绝。
- process tree 未证明退出时拒绝。
- unlock crash 恢复。
- active cleanup operation 与 fact-only disposition 每个 phase 的 crash/known-failure 映射。
- remove 不使用 force。
- worktree Removed 后 branch 默认 Retained。
- current ancestry 失效拒绝 branch delete。
- CAS ref drift 不误删。
- target ancestry check 与 source delete 使用单个 `verify target + delete source` ref transaction，覆盖检查后 target reset 竞态。
- branch absent 的 lost reply 收敛 Deleted。
- restart 时 Removed/Deleted disposition 不被 P4-A reconciler 误判为 artifact missing/inconsistent。
- 完整 P4-A `recover_after_restart`、committed high watermark、EventDispatcher、StoreWriter 和 bootstrap 顺序未被 P4-B 插入阶段跳过。

### 23.8 API/security/Web

- session/Origin/CSRF/mutation gate。
- request bounds/unknown fields/idempotency。
- stale modal generation/head。
- exact status/error/OpenAPI。
- polling/reload恢复。
- conflict UI 和两个 cleanup confirmation。
- 既有 11 task events/SSE replay 无变化。
- UI 不显示绝对路径、stderr 或原始 evidence 内容。

### 23.9 offline E2E

- Approved task 在用户点击前不产生 source commit/merge。
- 显式 merge 后目标 clean、产生 no-ff commit。
- historical `Completed + Unreviewed` 不可 merge。
- dirty 原 checkout 拒绝且用户文件不变。
- target head stale 后重新 preflight。
- conflict target 不变；外部修复/推进后新 operation 成功。
- merge 后显式 remove worktree，再显式 delete branch。
- source/merge/cleanup 各关键 crash point 重启收敛。
- 双击/HTTP reply lost 不重复 side effect。

## 24. 完成门

只有以下条件全部满足，P4-B 才可报告完成：

1. 本规格获用户书面批准。
2. 分步 TDD 实施计划获用户书面批准。
3. 按计划完成 RED/GREEN/REFACTOR，未加入 P4-C/P4-D 能力。
4. source/merge/conflict/cleanup/recovery 状态机有 fault-injection 证据。
5. 六态 Task、readiness、artifact 和 11 种 task event 回归全绿。
6. 用户原 checkout dirty/HEAD drift/外部 Git race 全部 fail closed。
7. 完整 Rust workspace fmt/clippy/test 全绿。
8. Web API/config/typecheck/unit/build 全绿。
9. OpenAPI/TypeScript drift 检查全绿。
10. Playwright P4-B 和既有 E2E 全绿。
11. embedded production release build/smoke 全绿。
12. placeholder、敏感信息和 `git diff --check` 全绿。
13. 独立代码审查无未解决问题。
14. 验收演示明确证明“未点击不 merge、冲突不改目标、cleanup 分两步”。

## 25. 实施完成与最终验收记录

本规格与 P4-B 分步 TDD 实施计划已经用户书面批准，实施授权自 2026-08-04 生效。P4-B 已按批准边界完成实现、独立审查和任务 30 全部门禁；2026-09-01，Project 4（P4-A + P4-B）完成最终验收。

最终代码候选、CI run、7 个必需 job、Windows 本地门禁、独立审查与范围核对的不可变证据统一记录在 [P4-B 实施计划的“实施完成与最终验收记录”](../plans/2026-08-04-controlled-delivery-merge-cleanup.md#实施完成与最终验收记录)，本规格不复制第二份易漂移清单。该记录只证明三文档封存提交的父级代码候选，不证明封存提交本身；封存提交自身的 7 个必需 CI 只能在该提交产生并完成运行后于仓库外的最终交付记录中报告，不能回写本规格而再制造一个尚未验证的新 HEAD。P4-C/P4-D 仍是未来独立项目，不属于本次验收欠账。
