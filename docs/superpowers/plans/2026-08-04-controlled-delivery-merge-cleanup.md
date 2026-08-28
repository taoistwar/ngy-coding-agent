# Project 4B：受控本地交付、合并与 Git 现场清理实施计划

> 日期：2026-08-04
> 状态：规格与本计划已于 2026-08-04 获用户书面批准；P4-B 实施进行中
> 基线：`29b81d9 project 4 P4-A：受控并发与资源准入`
> 执行规则：按任务顺序手工执行 TDD；每个任务先 RED、再 GREEN、最后 REFACTOR 与回归验证

**目标：** 在不扩展六态 `TaskStatus`、不改变 `DeliveryReadiness`、attempt artifact 和 11 种持久 task event 语义的前提下，为 `Completed + ReviewApproved` 任务增加用户显式触发、可恢复且 fail-closed 的本地 source commit、固定 `--no-ff` merge、冲突恢复，以及分两步确认的 worktree/source-branch 清理。

**源规格：** `docs/superpowers/specs/2026-08-04-controlled-delivery-merge-cleanup-design.md`，已于 2026-08-04 获用户书面批准。规格与本计划冲突时以规格为准，停止实现并修订计划，不能现场发明行为。

**架构：** 依赖方向继续锁定为 `app -> {api,store,core,provider,runtime}`、`{provider,runtime} -> core -> domain`。SQLite v5、command receipt、current row、transition journal 和一致资格快照归 `store`；Git capability、观察、tree/commit/merge/abort/cleanup 归 `runtime`；operation actor、repository lease、恢复、poison、startup/shutdown 和投影归 `app`；HTTP/OpenAPI 归 `api`；React 只消费 typed REST projection 并 polling。

**技术栈：** Rust 1.97、edition 2024、Tokio、Serde、SQLx/SQLite、Axum/OpenAPI、Git >= 2.45、React 19、TypeScript、Vite、Vitest、Playwright。验证默认离线，使用临时真实 Git/Cargo 仓库和 scripted provider，不联系真实 provider。

## 全局执行约束

- 本目录沿用历史文档组织方式，不表示仓库依赖或启用了 `superpowers` 工作流。
- 每个任务严格执行 RED -> GREEN -> REFACTOR：先写聚焦失败测试并确认失败原因，再实现最小完整行为，最后运行聚焦测试和受影响回归。
- 每个任务结束检查 `git status --short`、聚焦 diff、敏感信息和 `git diff --check`；不覆盖、回退或整理用户无关改动。
- 生产期 SQLite mutation 只能通过唯一 `StoreWriter`。migration、single-instance lock 内的 startup ownership read、非 delivery-owned `Reserved` artifact reconciliation 和 P4-A cold recovery 是既有 direct Store 例外，不得扩大。
- `TaskStatus` 保持六态；`DeliveryReadiness`、review evidence、attempt artifact 与 delivery/cleanup 状态继续分离；既有 11 种 persisted task event、`schema_version=1`、`last_event_id` 和 SSE cursor 不变。
- `Completed + Unreviewed`、`ReviewRejected`、非 `Completed`、evidence/fingerprint drift、active process tree 或 artifact 非 `Ready` 永远不能进入 merge side effect。
- 每个 POST 使用独立、全局唯一 `client_request_id` 和 immutable command receipt；preflight receipt、merge-accept receipt、remove-worktree receipt、delete-branch receipt不能互相覆盖。
- 用户未点击时不创建 source commit、不 merge、不 cleanup；启动恢复、Scheduler tick、Reviewer approval、GET、页面刷新和 polling 都不得创建新用户 operation。
- source/target/cleanup Git mutation 必须持有同一个 `RepositoryControlCoordinator` non-blocking lease；StoreWriter 不获取 lease，SQLite transaction 不跨 Git child，runtime callback 不反向调用 actor/StoreWriter。
- command outcome、process-tree cleanup、Store receipt 或 Git identity无法证明时不得猜测；按规格进入 pending recovery 或 `ReconciliationRequired + poison`。
- 所有 P4-B Git 命令复用 P4-A capability binding、固定 executable/cwd/env/argv/deadline/output bounds 和 process supervision；不接受 shell string、任意 argv/path/ref/message/author。
- 固定禁用或拒绝 hook、filter、external diff、custom merge driver、fsmonitor、submodule、signing、signature verification、editor、autostash、rerere、branch merge options、replace objects 和 lazy fetch。
- ignored-untracked collision、dirty target、detached HEAD、HEAD/config/attribute drift和 ongoing Git operation必须在目标零副作用下拒绝；实际 merge仍固定 `--no-overwrite-ignore`。
- source commit 和 expected merge commit 的 tree、parents、author/committer、UTC 整秒 `+0000`、message bytes 和 OID 都必须可确定重放；source pending 时禁止出现 terminal `Failed` merge。
- `evidence_identity_v1` 固定绑定 task/repository/attempt、final review round/event、generation、workspace fingerprint、checks digest 和 coverage digest；只从Store中既有canonical evidence计算，不相信HTTP重复字段。
- common Git 与 worktree admin 使用可跨重启重算的 `directory_identity_v1` domain-separated SHA-256摘要分别持久化；不得复用仅进程内随机/opaque marker，也不得进入API、日志或错误。
- cleanup disposition只记录已证明事实；pending intent只存在 cleanup operation。worktree remove不使用force；branch delete使用同一 ref transaction内的 target verify + source CAS delete。
- delivery-owned artifact在 startup、GET projection 和 recovery 前先做 typed ownership join，不进入要求 `branch=base + worktree present` 的 P4-A ready observer。
- P4-B 继续使用 REST polling；不得增加第 12 种 task event、修改 SSE union或把 delivery operation混入 lifecycle event。
- 不增加 rebase、cherry-pick、squash、fast-forward选择、fetch/pull/push、PR、远程认证、自动冲突解决、自动 cleanup、P4-C history/artifact lifecycle或P4-D packaging/settings/provider能力。
- 新源码按 `AGENTS.md` 以职责和不变量拆分。orchestrator方法只保留阶段编排；验证、命令构造、状态分类和错误映射使用单职责方法，禁止把全部 P4-B 堆进现有大文件。
- 所有公开错误稳定、短且脱敏；不得返回或记录 secret、prompt、diff、Git stderr、原始 config、环境、绝对路径或冲突文件内容。
- Windows 长测试若遇共享 build/SQLite lock，先确认错误性质，再串行重跑并等待真实 exit code；不能把超时或静默输出解释为通过。

## 锁定的归属映射

```text
crates/coding-agent-store/
  migrations/0005_controlled_delivery.sql
  src/delivery/{mod,types,values,state,error,records,evidence,eligibility,ownership,
                receipts,sources,merges,cleanup,recovery,transitions}.rs
  tests/delivery_{types,eligibility,receipts,sources,merges,cleanup,recovery}.rs
  tests/{migrations,artifacts,reviews,projection,recovery}.rs

crates/coding-agent-runtime/
  src/command_policy/git_delivery.rs
  src/process_supervisor/input.rs
  src/worktree/authentication.rs
  src/delivery/{mod,types,probe,observation,config,attributes,command,source_tree,source_commit,target,
                preflight,merge,abort,cleanup,recovery}.rs
  tests/delivery_{source,target,preflight,merge,abort,cleanup,security,recovery}.rs
  src/{command_policy,worktree,process_supervisor,lib}.rs

crates/coding-agent-app/
  src/delivery_manager/{mod,command,query,preflight,source,merge,abort,cleanup,recovery,shutdown}.rs
  src/delivery_api_projection/{mod,dto,logical}.rs
  src/delivery_reconciliation.rs
  src/{runner_factory,single_instance/start_primary,shutdown}/delivery.rs
  src/store_writer/command/delivery.rs
  src/{artifact_reconciliation,repository_control,service_state,single_instance,shutdown,server,test_support,lib}.rs
  tests/delivery_{manager,merge,cleanup,recovery,startup,shutdown}.rs
  tests/{artifact_reconciliation,repository_control,server,single_instance,shutdown,offline_e2e}.rs

crates/coding-agent-api/
  src/contract/delivery_contract.rs
  src/delivery_wire/{mod,validation}.rs
  src/{backend,router,error,lib}.rs
  tests/{delivery,openapi,router,sse}.rs

web/
  openapi.json
  src/api/{authenticatedTransport,deliveryClient,deliveryValidation}.ts and tests
  src/api/{client,types}.ts and tests
  src/api/generated/schema.d.ts
  src/state/{deliveryModel,deliveryReducer,useDeliveryPolling}.ts and tests
  src/components/DeliveryPanel/{index,Eligibility,PreflightModal,MergeProgress,CleanupControls}.tsx
  src/components/DeliveryPanel.test.tsx
  src/components/TaskWorkspace.tsx and tests
  src/styles.css
  e2e/{controlled-delivery,delivery-recovery}.spec.ts

README.md
```

实现可采用同职责下更小的模块，但不得反转 crate 依赖、把 Git 类型放进 domain、把持久事实放进 app 临时字符串，或重新合并成单体文件/大方法。

## Checkpoint A：typed 持久状态、SQLite v5 与线性化原语

## 任务 1：建立 delivery persistence typed state 与 exact validator

**文件：**

- 新建 `crates/coding-agent-store/src/delivery/{mod,types,values,state,error}.rs`
- 新建 `crates/coding-agent-store/tests/delivery_types.rs`
- 新建 `crates/coding-agent-domain/tests/p4b_boundary.rs`
- 修改 `crates/coding-agent-store/src/lib.rs`

- [x] RED：覆盖 source、merge、worktree disposition、branch disposition和两类 cleanup operation的全部合法/非法状态转换；锁定 open、side-effect-active、terminal和reconciliation分类。
- [x] RED：覆盖 canonical UUID、task/repository/attempt identity、SHA-1/SHA-256 OID 40/64 lowercase hex、fingerprint、tree/parent、UTF-8 `refs/heads/*`、timestamp、version和bounded failure code。
- [x] RED：固定 `evidence_identity_v1` exact fields/canonical digests、`directory_identity_v1` algorithm+64hex encoding、initial transition `from=absent/version=1` 和每次version恰好+1。
- [x] RED：证明 `Failed merge + ObjectPending|CommitPending source`、pending disposition、错误 cleanup kind/state组合和 backward transition均被 typed validator拒绝。
- [x] RED：domain characterization exhaustive锁定6种TaskStatus和11种TaskEventKind/Payload，不存在Merged status或delivery event。
- [x] GREEN：实现私有字段 validated newtypes、exact enum parse/serialize、单向 transition helper和稳定 `StoreError`；不向 `coding-agent-domain` 增加 Git/SQLite 类型。
- [x] REFACTOR：按 identity、state、bounded wire value拆分小模块；删除测试专用字符串捷径。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test delivery_types --locked --offline -- --nocapture
cargo test -p coding-agent-domain --test p4b_boundary --locked --offline
cargo check -p coding-agent-store --all-targets --locked --offline
```

检查点：typed state先于 SQL 和 actor，后续层不能重新发明字符串状态机。

## 任务 2：增加 SQLite v5 schema、约束与迁移兼容

**文件：**

- 新建 `crates/coding-agent-store/migrations/0005_controlled_delivery.sql`
- 修改 `crates/coding-agent-store/src/{migrate,lib}.rs`
- 新建 `crates/coding-agent-store/tests/migrations_v5.rs`
- 新建 `crates/coding-agent-store/tests/support/{delivery,migration_v5}.rs`
- 修改 `crates/coding-agent-store/tests/{migrations,support/mod}.rs`
- 新建 `crates/coding-agent-store/tests/delivery_types.rs` 中的 raw-SQL corruption cases

- [x] RED：覆盖空库、真实 v1/v2/v3/v4 -> v5、v5重开、重复 migrate、future/gap/非法 history、逐 SQL 故障回滚和 `PRAGMA foreign_key_check`。
- [x] RED：用 raw SQL覆盖七张 current/receipt/journal 表的 STRICT、FK、CHECK、length、immutable identity、kind/state和fact-only disposition约束。
- [x] RED：锁定同 Task open/side-effect-active merge partial unique、同 Task唯一 `Merged`、同 artifact/disposition唯一 active cleanup，以及 `Merged`/`ReconciliationRequired` 对新 preflight的DB级阻断。
- [x] RED：证明 `Merged` transition与初始 `RetainedLocked + Retained` disposition必须同事务存在；证明 `Failed merge + pending source` 和缺失/矛盾 ownership join不能通过 trigger/transition API。
- [x] RED：receipt/operation循环引用使用deferred FK或等价trigger闭合；receipt、journal、conflict rows append-only，禁止UPDATE/DELETE/REPLACE和孤儿。
- [x] RED：raw SQL与并发测试锁定exact `UNIQUE(command_kind, operation_id)`；preflight、accept、remove-worktree、delete-branch各自恰有一个成功origin receipt，两个不同`client_request_id`不能绑定同一kind+operation。
- [x] RED：receipt中的accepted operation version/state/response discriminator必须与同事务current row和journal精确一致；伪造版本、状态、kind或响应类别由DB约束拒绝。
- [x] RED：v1-v4升级不回填虚假 delivery row，不改写 Task/readiness/review/artifact/event，11种 event仍为wire schema 1。
- [x] GREEN：实现 `task_delivery_sources`、`task_merge_operations`、`task_merge_conflicts`、`task_artifact_dispositions`、`task_cleanup_operations`、`task_delivery_command_receipts` 和 `task_delivery_operation_transitions` 及必要 index/trigger。
- [x] REFACTOR：migration version 5与wire/event schema 1保持不同命名空间；SQL错误不泄露数据库路径或值。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test migrations --locked --offline -- --nocapture
cargo test -p coding-agent-store --test migrations_v5 --locked --offline -- --nocapture
cargo test -p coding-agent-store --test delivery_types --locked --offline -- --nocapture
cargo test -p coding-agent-store --test artifacts --locked --offline
cargo test -p coding-agent-store --test reviews --locked --offline
```

检查点：v5只增加 P4-B 表和约束，不改变历史 Task/event 字节语义。

## 任务 3：实现一致 eligibility snapshot 与 delivery ownership join

**文件：**

- 新建 `crates/coding-agent-store/src/delivery/{eligibility,ownership}.rs`
- 新建 `crates/coding-agent-store/src/delivery/{records,evidence}.rs`
- 新建 `crates/coding-agent-store/tests/delivery_eligibility.rs`
- 修改 `crates/coding-agent-store/src/{artifacts,reviews,lib}.rs`
- 修改 `crates/coding-agent-store/tests/{artifacts,reviews,projection}.rs`

- [x] RED：在单个一致 read transaction中返回 Task、attempt、readiness、最终 review generation/fingerprint/check/coverage identity、artifact identity/state、source、merge和cleanup current rows。
- [x] RED：覆盖 `Completed + ReviewApproved` 正例，以及 historical Unreviewed、Rejected、非Completed、stale generation/fingerprint/check/coverage、artifact非Ready、成功Merged和reconciliation阻断。
- [x] RED：覆盖 absent、合法 delivery-owned、Committed source、Removed/Deleted disposition，以及 P4-B row引用 Reserved/Inconsistent、attempt mismatch或缺 artifact的矛盾 join。
- [x] RED：snapshot生成 `evidence_identity_v1`，source/operation provenance只能从同一transaction复制/校验，不接受调用方或HTTP提供的digest替代。
- [x] GREEN：实现 `DeliveryEligibilitySnapshot` 与 `DeliveryOwnershipSnapshot`；Store只投影持久事实，TaskManager ownership、process tree和fresh Git observation留给app/runtime完成。
- [x] REFACTOR：复用既有 review/artifact row decoder，不做多次近似查询；错误不包含 prompt、diff或路径。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test delivery_eligibility --locked --offline -- --nocapture
cargo test -p coding-agent-store --test artifacts --locked --offline
cargo test -p coding-agent-store --test reviews --locked --offline
cargo test -p coding-agent-store --test projection --locked --offline
```

检查点：Store资格快照是一个事务快照，但不谎称已证明进程或Git现场。

## 任务 4：实现 command receipt、preflight create 与 stale/supersede 事务

**文件：**

- 新建 `crates/coding-agent-store/src/delivery/{receipts,merges,transitions}.rs`
- 新建 `crates/coding-agent-store/tests/delivery_receipts.rs`
- 修改 `crates/coding-agent-store/src/lib.rs`

- [x] RED：每个 action独立 canonical request hash；相同 UUID+kind/hash返回 Existing，相同 UUID跨 action或不同hash返回 `IDEMPOTENCY_CONFLICT`。
- [x] RED：锁定domain/version/framing和逐action字段集：preflight绑定task+target branch/head；accept另绑定preflight operation/version+expected evidence；两个cleanup各绑定task+action+expected disposition/source/merge/target identity。任一安全字段单独变化都冲突。
- [x] RED：hash只从exact validated typed request构造，不hash原始HTTP bytes；JSON key顺序/空白等语义等价编码得到同hash，ref/OID/UUID canonicalization后才进入framing。
- [x] RED：首次 preflight在同一事务写 receipt、`PreflightPending` current row和journal；reply lost后按receipt返回同一operation/version。
- [x] RED：对preflight origin、accept和两类cleanup逐一覆盖两个不同UUID竞争绑定同一operation；只允许一个事务成功，失败者不留receipt/journal/current副作用。
- [x] RED：已有 Pending仅同receipt重放；新请求得到 `DELIVERY_OPERATION_IN_PROGRESS`。已有Ready时同事务CAS为Superseded并创建新Pending；与accept并发只有一个version获胜。
- [x] RED：stale evidence/target/source把Ready原子写Stale；已有Merged、side-effect-active或ReconciliationRequired时创建事务零副作用拒绝。
- [x] GREEN：实现 command receipt lookup/insert、canonical hash、preflight create/supersede/stale typed operation和current+journal原子事务。
- [x] REFACTOR：Existing/query-first逻辑集中，禁止API预查后再写的TOCTOU路径。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test delivery_receipts --locked --offline -- --nocapture
cargo test -p coding-agent-store --test delivery_eligibility --locked --offline
cargo test -p coding-agent-store --test migrations --locked --offline
```

检查点：两个POST阶段不能覆盖同一idempotency字段，stale Ready不会永久占位。

## 任务 5：实现 source ObjectPending/CommitPending/Committed 事务

**文件：**

- 新建 `crates/coding-agent-store/src/delivery/sources.rs`
- 新建 `crates/coding-agent-store/tests/delivery_sources.rs`
- 修改 `crates/coding-agent-store/src/delivery/{types,transitions}.rs`

- [x] RED：`ObjectPending`必须一次性绑定task/repository/attempt/evidence/artifact/common identity、candidate tree、parent、exact author/committer date bytes、message bytes和template version。
- [x] RED：只有verified exact commit OID/shape可进入CommitPending；只有expected source ref/index/worktree proof可进入Committed；identity/provenance不可变。
- [x] RED：source pending的已知未应用错误只更新bounded retryable diagnostic/version，不把merge写Failed；unknown/mismatch只进入ReconciliationRequired。
- [x] RED：覆盖本任务direct Store事务的commit-before-reply、rollback和busy点，query-first重放不重复transition/journal；本阶段没有channel边界，production StoreWriter的channel-close矩阵保留在任务19。
- [x] GREEN：实现source create/advance/retry-diagnostic/reconcile transaction和exact receipt query。
- [x] REFACTOR：把immutable provenance、mutable phase outcome和recovery query分开，避免单个超大row mapper方法。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test delivery_sources --locked --offline -- --nocapture
cargo test -p coding-agent-store --test delivery_receipts --locked --offline
cargo test -p coding-agent-store --test migrations --locked --offline
```

检查点：source OID在修改真实index/ref之前已经durable，pending永远可按持久metadata解释。

## 任务 6：实现 merge accept、expected OID、conflict/abort 与 Merged 原子事务

**文件：**

- 修改 `crates/coding-agent-store/src/delivery/{merges,transitions}.rs`
- 新建 `crates/coding-agent-store/tests/delivery_merges.rs`
- 修改 `crates/coding-agent-store/tests/delivery_receipts.rs`

- [x] RED：accept receipt与 `PreflightReady -> Accepted` 同事务，绑定expected version/evidence/target；Stale/Superseded/Conflict/Rejected不能accept。
- [x] RED：Accepted在source Committed后才允许持久exact source commit、candidate tree、两parents、metadata/message和expected merge OID进入MergePending。
- [x] RED：known conflict写AbortPending时必须绑定child receipt、MERGE_HEAD、index stages、worktree digest和`MERGE_AUTOSTASH=absent` proof；无durable conflict receipt不能从MergePending推断abort。
- [x] RED：覆盖 MergePending -> Merged、AbortPending -> Conflict、known-zero-target-effect -> Failed和任意unknown -> ReconciliationRequired的合法矩阵。
- [x] RED：Merged current/journal与初始fact disposition row同事务；任一点fault全回滚，reply lost后只补写不重复Git side effect。
- [x] RED：冲突路径最多128、单值4096 bytes、总payload64KiB、ordinal唯一和UTF-8/base64url exact encoding。
- [x] GREEN：实现accept、merge-pending、abort-pending、conflict、failed、merged/reconcile事务及bounded conflict child rows。
- [x] REFACTOR：将state CAS、immutable shape验证和journal insert拆成可复用小helper，禁止泛化为任意状态update。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test delivery_merges --locked --offline -- --nocapture
cargo test -p coding-agent-store --test delivery_receipts --locked --offline
cargo test -p coding-agent-store --test delivery_sources --locked --offline
```

检查点：target mutation的expected commit OID在MergePending前已持久，Merged不会缺少cleanup事实初值。

## 任务 7：实现 fact-only disposition 与 cleanup operation 事务

**文件：**

- 新建 `crates/coding-agent-store/src/delivery/cleanup.rs`
- 新建 `crates/coding-agent-store/tests/delivery_cleanup.rs`
- 修改 `crates/coding-agent-store/src/delivery/{receipts,transitions}.rs`

- [x] RED：首次remove-worktree receipt在`RetainedLocked`时创建`UnlockPending + RetainedLocked`，依次只允许`UnlockedPendingRemove + RetainedUnlocked`、`RemovePending + RetainedUnlocked`、`Completed + Removed`。
- [x] RED：先前remove已知未应用而留下`Failed + RetainedUnlocked`时，fresh validation与新receipt必须直接创建`RemovePending + RetainedUnlocked`；禁止回到UnlockPending、重新lock或再次unlock。
- [x] RED：delete-branch receipt创建 `DeletePending + Retained`，绑定source OID、common identity、target ref/fresh head；只允许Completed+Deleted、known-not-applied Failed+Retained或ReconciliationRequired。
- [x] RED：active cleanup partial unique精确覆盖四个pending state；Failed后fresh新receipt可重试，Existing仍返回原operation。
- [x] RED：known no-effect failure保持事实disposition不回退；dirty remove保持RetainedUnlocked，target不再包含source保持Retained并返回稳定错误。
- [x] RED：operation/disposition两侧version与journal同事务；raw SQL不能制造pending disposition或kind/state错配。
- [x] RED：delete target HEAD refresh逐operation version持久不可变证据；多次refresh后旧版exact replay返回Existing，同版不同HEAD冲突，missing/orphan/mismatch fail closed。
- [x] GREEN：实现两类cleanup acceptance、phase advance、known failure、expected-target refresh、complete和reconcile transaction。
- [x] REFACTOR：共享receipt/transition原语但保持worktree与branch不变量分离，不做一个布尔参数大函数。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test delivery_cleanup --locked --offline -- --nocapture
cargo test -p coding-agent-store --test delivery_receipts --locked --offline
cargo test -p coding-agent-store --test delivery_merges --locked --offline
```

检查点：disposition只说已证明事实，pending intent不会因崩溃丢失或被误认为需要第二次确认。

## 任务 8：实现 ordered recovery query、transition journal 与 v1-v5回归

**文件：**

- 新建 `crates/coding-agent-store/src/delivery/recovery.rs`
- 新建 `crates/coding-agent-store/tests/delivery_recovery.rs`
- 新建 `crates/coding-agent-store/tests/delivery_event_compatibility.rs`
- 修改 `crates/coding-agent-store/src/{recovery,lib}.rs`
- 修改 `crates/coding-agent-store/tests/{recovery,projection}.rs`

- [x] RED：按 authenticated common Git identity、operation creation order返回全部PreflightPending、Accepted、source pending、MergePending、AbortPending和active cleanup；terminal不被错误重放。
- [x] RED：ownership join缺失/矛盾、current/journal version gap、receipt/action mismatch、Merged缺disposition和reconciliation row均fail closed。
- [x] RED：current row与journal在逐步fault injection、busy、rollback、commit-before-reply后保持exact一致，重复recovery query稳定。
- [x] RED：P4-A `recover_after_restart`、Task event high watermark、Queued/Running语义和11种event projection不因delivery表出现而改变。
- [x] RED：对source/merge/conflict/cleanup每一种transition快照比较tasks、readiness、review、artifact、11 event rows和last_event_id，delivery write前后逐字节不变。
- [x] GREEN：实现bounded recovery batch/query DTO、journal consistency audit和startup ownership read；不在Store层执行Git或决定外部identity。
- [x] REFACTOR：把P4-A lifecycle recovery与P4-B operation recovery保持两个明确阶段，共享连接但不嵌套事务。
- [x] 验证：

```powershell
cargo test -p coding-agent-store --test delivery_recovery --locked --offline -- --nocapture
cargo test -p coding-agent-store --test recovery --locked --offline -- --nocapture
cargo test -p coding-agent-store --test projection --locked --offline
cargo test -p coding-agent-store --test migrations --locked --offline
cargo test -p coding-agent-store --test delivery_event_compatibility --locked --offline
```

检查点：Store层完成后，所有durable intent都有唯一typed恢复入口且不产生新task event。

## Checkpoint B：capability-bound Git runtime 与可证明副作用

## 任务 9：增加 bounded exact child stdin 与 pre-DB Git capability probe

**文件：**

- 新建 `crates/coding-agent-runtime/src/process_supervisor/input.rs`
- 新建 `crates/coding-agent-runtime/src/command_policy/git_delivery.rs`
- 新建 `crates/coding-agent-runtime/src/delivery/probe.rs`
- 新建 `crates/coding-agent-runtime/tests/{process_stdin,delivery_probe}.rs`
- 修改 `crates/coding-agent-runtime/src/{process_supervisor,command_policy,lib}.rs`
- 修改 `crates/coding-agent-app/src/single_instance/start_primary.rs`
- 修改 `crates/coding-agent-app/tests/single_instance.rs`

- [x] RED：现有command默认仍使用null stdin；只有crate-private validated delivery command可携带bounded `ExactChildInput`，超限在spawn前拒绝。
- [x] RED：exact input写完必须关闭stdin；payload bytes不进入Debug、日志、错误、trace或child argv，commit message和ref transaction逐字节到达fixture child。
- [x] RED：区分stdin write/close成功、child early-exit/broken-pipe、timeout、cancel、wait/channel unknown和process-tree cleanup-unproven；writer task不能泄漏或让未知结果变成普通command failure。
- [x] RED：应用私有临时probe repository实际证明Git >=2.45、object format、完整merge option set、merge-tree和update-ref transaction语法；probe目录/child清理无法证明时startup fail closed。
- [x] RED：Git capability probe在SQLite open/migrate之前完成；probe失败时StoreFactory open、recovery和listener bind调用数均为0。
- [x] RED：probe成功才返回字段私有、跨crate可传递但不可构造/伪造的`ProbedDeliveryGit` opaque handle；它绑定本次实际探测的同一个`Arc<PinnedExecutable>` identity/digest与capability结果，Git-A handle不能授权Git-B runtime或替换后的executable。
- [x] RED：所有delivery mutation runtime/command factory只能从`ProbedDeliveryGit`取得；没有handle、handle/executable identity不匹配或probe后executable替换时无法构造/执行Git delivery mutation。
- [x] GREEN：实现non-logging exact stdin capability、supervised writer lifecycle和pre-DB probe factory；factory直接返回绑定同一pinned executable的opaque delivery runtime handle，composition root只注入该handle，不传裸token。
- [x] REFACTOR：`process_supervisor.rs`只保留编排并委托input模块；无stdin command路径行为/性能不变。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test process_stdin --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test delivery_probe --locked --offline -- --nocapture
cargo test -p coding-agent-runtime process_supervisor --lib --locked --offline -- --nocapture
cargo test -p coding-agent-runtime command_policy --lib --locked --offline -- --nocapture
cargo test -p coding-agent-app --test single_instance --features test-support --locked --offline
```

检查点：没有受限exact stdin就不能实现commit-tree message或原子ref transaction，probe未通过就不能触碰数据库。

## 任务 10：建立 P4-B Git command policy 与 delivery source 观察

**文件：**

- 新建 `crates/coding-agent-runtime/src/delivery/{mod,types,config,command,observation}.rs`
- 新建 `crates/coding-agent-runtime/src/worktree/authentication.rs`
- 新建 `crates/coding-agent-runtime/tests/{delivery_source,delivery_security}.rs`
- 修改 `crates/coding-agent-runtime/src/{command_policy,root_capability,worktree,lib}.rs`

- [x] RED：所有delivery command复用pinned Git、common/admin/worktree capability、fixed `--git-dir/--work-tree`、no replace/lazy fetch、fsmonitor/untracked cache/submodule禁用和clean environment。
- [x] RED：`open_delivery_source` 接受exact reviewed dirty source，证明common/admin/path/branch/base HEAD、fixed lock reason、index无unmerged、无gitlink和approved fingerprint；P4-A `open_ready`仍拒绝dirty。
- [x] RED：拒绝 include/includeIf、`extensions.worktreeConfig`或任何admin `config.worktree`、executable filter/diff、custom merge driver、branch.*.mergeOptions、unsafe hooks/signing组合、config injection env和attributes外部程序；独立dirty-source入口必须保持P4-A既有拒绝边界，不能只扫描common config。
- [x] RED：覆盖`config.worktree`文件在观察前后创建、symlink/reparse或identity替换、扩展开关TOCTOU；一律在任何filter/driver/hook/helper和delivery mutation前fail closed且探针零调用。
- [x] RED：覆盖symlink/reparse-point、case/SUBST alias、admin/common-dir替换、lock reason drift、non-UTF8/非法ref、oversized status/config/attributes和command timeout。
- [x] 边界：Unix 空配置使用 `0600` create→unlink retained FD，防后续 namespace 替换或按路径重开；它不防御同一 UID 在 unlink 前已取得可写 FD，除非采用 OS-specific anonymous FD 或隔离。
- [x] RED：从已认证common Git directory和worktree admin的真实平台identity生成两个domain-separated `directory_identity_v1` SHA-256；同对象跨reopen/新进程稳定、合法alias相同、替换对象不同，Windows/Unix字段使用无歧义framing且两个domain不能碰撞。
- [x] RED：证明现有进程随机 `DirectoryIdentityMarker::opaque_hash` 不能进入任何durable row；新digest不进入Debug、API、日志或错误，只可在typed Store provenance中比较。
- [x] GREEN：实现独立 `DeliverySourceCapability`、delivery config/attribute digest、allowlisted environment和typed command builder；不向Agent Git tools暴露mutation。
- [x] GREEN：在 `worktree/authentication.rs` 实现 `DurableDirectoryIdentityV1`，从root capability已证明的platform fields构造common/admin digest；保留random marker仅作进程内快速identity用途。
- [x] REFACTOR：新命令放入delivery职责模块，认证基元从大 `worktree.rs` 拆出，既有 `command_policy.rs` 只保留共享primitive/委托。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test delivery_source --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test delivery_security --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test worktree --locked --offline
cargo test -p coding-agent-runtime --test path_security --locked --offline
cargo test -p coding-agent-runtime command_policy --lib --locked --offline -- --nocapture
```

检查点：P4-B拥有独立dirty-source入口，但没有放宽Planner/Executor/Reviewer或P4-A观察能力。

## 任务 11：实现临时 index、candidate tree 与 deterministic source object

**文件：**

- 新建 `crates/coding-agent-runtime/src/delivery/{source_tree,source_commit}.rs`
- 修改 `crates/coding-agent-runtime/src/delivery/{command,types,mod}.rs`
- 新建 `crates/coding-agent-runtime/tests/delivery_source_commit.rs`
- 修改 `crates/coding-agent-runtime/tests/delivery_security.rs`

- [x] RED：在真实index不变时，以private temp index从exact base执行`read-tree`；candidate只从no-follow、identity-bound snapshot的exact bytes/mode构造，固定使用`hash-object -w --no-filters --stdin`、typed `update-index --add --replace -z --index-info`和`write-tree`，精确覆盖tracked/untracked approved内容且不包含ignored/gitlink。
- [x] RED：temp-index `write-tree`完成后、Store写`ObjectPending`之前，必须重新认证真实index/worktree并重新采集no-follow snapshot/fingerprint，精确等于approved；在两次认证间插入文件/index变化时零durable intent、零真实index/ref副作用。
- [x] RED：temp index path不进入log/error/API；create/write/delete只作用于应用自建identity-matched文件，崩溃残留可安全识别。
- [x] RED：用persisted author/committer epoch-second +0000和exact ASCII+LF message bytes重放commit-tree，每次得到相同source OID；object shape逐字段验证。
- [x] RED：hook/editor/template/cleanup/signing/filter/fsmonitor/config injection均不执行；在candidate构造前注入config、`info/attributes`或worktree attributes时，filter/helper sentinel仍不得执行，随后fresh revalidation必须fail closed；stdout/stderr/timeout/channel unknown得到typed outcome。
- [x] GREEN：实现candidate-tree builder、deterministic commit-object builder和object inspector；ObjectPending之前不修改真实index/ref，Accepted/PreflightPending是tree/object写入的durable上层intent。
- [x] REFACTOR：把temp capability生命周期、command construction和object validation拆开，主方法只编排阶段。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test delivery_source_commit --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test delivery_source --locked --offline
cargo test -p coding-agent-runtime --test delivery_security --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test delivery_source_commit --features test-support --locked --offline -- --nocapture --test-threads=1
cargo test -p coding-agent-runtime --test delivery_security --locked --offline -- --nocapture --test-threads=1
```

检查点：相同persisted输入跨重启产生同一source OID，真实index/ref仍为零副作用。

## 任务 12：实现真实 source index/ref CAS 与 source recovery classifier

**文件：**

- 修改 `crates/coding-agent-runtime/src/delivery/source_commit.rs`
- 新建 `crates/coding-agent-runtime/src/delivery/recovery.rs`
- 新建 `crates/coding-agent-runtime/tests/delivery_recovery.rs`
- 修改 `crates/coding-agent-runtime/tests/delivery_source_commit.rs`

- [x] RED：CommitPending后fresh revalidation必须重新证明source committed前置现场：source identity/fixed lock/HEAD、approved fingerprint、config/attributes digest、candidate的`tree` type proof和expected object shape；成功后才以固定`read-tree --reset <candidate>`（无`-u`）写入真实index，紧接固定`update-index --refresh -q`只刷新stat cache，并以`diff-index` predicate证明其精确等于candidate，CAS `update-ref source expected base`前再次验证object shape。若停在stage与refresh之间，纯观察只能保守reconcile，不能为恢复而写真实index。
- [x] RED：覆盖runtime side effect前、stage部分/完成、CAS前、CAS成功后和post-verify前的crash point；Store reply属于后续应用层StoreWriter边界，不在runtime直接伪造。
- [x] RED：恢复先按durable state分流：`ObjectPending`只接受source ref=base且真实index/worktree仍为approved pre-stage fingerprint，随后只重放deterministic object并推进CommitPending；即使现场恰为candidate-staged或expected-source也必须ReconciliationRequired。runtime recovery intent只能从已认证source、typed candidate和（如有）expected source capture opaque common/admin evidence；每次fresh bind精确比较，不能由原始持久字段公开构造。
- [x] RED：只有`CommitPending`接受三种组合：base+approved pre-stage、base+candidate index/worktree一致、expected source+clean exact commit；跨状态组合和其余ref/tree/index/worktree/config组合全部ReconciliationRequired。
- [x] RED：外部文件、index、branch、lock/admin/common identity漂移时不reset/clean/checkout；known未应用error保持可重试pending。repository lease只覆盖本应用；最终revalidation与`read-tree`取得Git index lock之间的非协作外部index writer不提供原子保留保证，后续可观察漂移必须reconcile，真正index CAS/ownership留作单独设计。
- [x] GREEN：实现以exact candidate tree写入真实index的source apply和纯观察classifier，返回continue/stage-complete/applied/reconciliation typed disposition，不直接写Store或poison；跨进程Store record到runtime recovery intent的受信adapter明确留在Task 21，不在Task 12宣称完成。
- [x] REFACTOR：把fresh identity、index tree、ref CAS、postcondition和recovery classification各自抽成单职责方法。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test delivery_source_commit --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test delivery_recovery --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test fingerprint --locked --offline
cargo test -p coding-agent-runtime --test worktree --locked --offline
```

检查点：source ref mutation恰好一次；无法证明时保留现场并交由上层poison。

## 任务 13：实现 target checkout 观察、ignored collision 与 merge-tree preflight

**文件：**

- 新建 `crates/coding-agent-runtime/src/{target_checkout.rs,delivery/{collision,target,preflight}.rs}`
- 新建 `crates/coding-agent-runtime/tests/{delivery_target,delivery_preflight}.rs`
- 修改 `crates/coding-agent-runtime/src/{command_policy/git_delivery.rs,delivery/{config,command,observation,source_commit,types,mod}.rs}`

- [x] RED：只接受登记repository path当前symbolic local branch和exact HEAD；detached、branch mismatch、dirty含untracked、unmerged index和merge/rebase/cherry-pick/revert/bisect状态零副作用拒绝。
- [x] RED：对candidate write-set及父级检测ignored-untracked file/dir/symlink collision；preflight与后续actual命令之间新增collision仍由`--no-overwrite-ignore`保护。
- [x] RED：source未Committed时使用ephemeral candidate commit，已Committed时使用exact persisted source；`merge-tree --write-tree --messages --name-only -z` clean/conflict结果不修改任何ref/index/file。
- [x] RED：source已是target ancestor稳定 `SOURCE_ALREADY_IN_TARGET`；target head/evidence/source fingerprint drift返回typed stale/rejection，由上层将已有preflight operation写Stale，而非自动merge。
- [x] RED：conflict path exact UTF-8/base64url、count/size bounds；malformed/oversized/unknown output fail closed且不返回stderr/content。
- [x] GREEN：实现target capability、write-set/ignored scan、preflight object/merge-tree parser和pure result classifier。
- [x] REFACTOR：observation、collision、command和parser分文件；避免用porcelain human text或path string拼shell。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test delivery_target --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test delivery_preflight --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test delivery_security --features test-support --locked --offline -- --nocapture --test-threads=1
```

检查点：preflight最多写不可达objects，target checkout逐字节保持不变。

## 任务 14：实现 expected merge object 与固定 no-ff actual merge

**文件：**

- 新建 `crates/coding-agent-runtime/src/delivery/merge.rs`
- 新建 `crates/coding-agent-runtime/tests/delivery_merge.rs`
- 修改 `crates/coding-agent-runtime/src/delivery/{command,types,mod}.rs`

- [x] RED：Accepted绑定candidate tree、target第一parent、source第二parent、UTC秒+0000和exact message；commit-tree重放得到同一expected merge OID。
- [x] RED：actual argv精确含 `--no-ff --strategy=ort --no-edit --no-verify --no-verify-signatures --no-gpg-sign --no-autostash --no-rerere-autoupdate --no-overwrite-ignore --no-log --no-stat --cleanup=verbatim`、fixed message、`--`和source OID。
- [x] RED：恶意 `branch.*.mergeOptions` 拒绝，`merge.verifySignatures=true` 被exact override/CLI中和且不启动GPG/SSH程序；hooks/editor/signing/autostash/rerere/custom driver均零执行。
- [x] RED：postcondition要求HEAD=expected OID、tree=candidate、parents顺序/metadata/message精确、target clean、无merge state；Already-up-to-date和不同OID不能伪装成功。
- [x] RED：expected object构造可能耗时；actual merge child spawn前必须再次认证source ref=Committed exact OID、source worktree clean/fixed lock、target symbolic branch/exact old HEAD/clean/config、ancestry和ignored collision。任一漂移零actual-merge副作用并返回typed stale/rejection。
- [x] RED：command known-zero-effect、conflict、timeout、wait/channel unknown和postcondition mismatch分别返回typed outcome，不运行reset/clean。
- [x] GREEN：实现expected merge object builder、fixed merge command和exact postcondition inspector。
- [x] REFACTOR：共享source/merge commit metadata primitive，但保留不同template/version和parent不变量。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test delivery_merge --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test delivery_preflight --locked --offline
cargo test -p coding-agent-runtime --test delivery_security --locked --offline -- --nocapture
```

检查点：Git实际创建的merge commit必须与mutation前persisted exact OID相同。

## 任务 15：实现 unexpected conflict、exact abort 与 merge recovery classifier

**文件：**

- 新建 `crates/coding-agent-runtime/src/delivery/abort.rs`
- 修改 `crates/coding-agent-runtime/src/delivery/recovery.rs`
- 新建 `crates/coding-agent-runtime/tests/delivery_abort.rs`
- 修改 `crates/coding-agent-runtime/tests/{delivery_merge,delivery_recovery}.rs`

- [x] RED：只有known conflict child outcome、old HEAD、exact MERGE_HEAD=source、index stages/worktree digest一致且MERGE_AUTOSTASH明确absent才生成可持久AbortPending proof。
- [x] RED：每次abort retry前重验proof和MERGE_AUTOSTASH absent；存在/不可观察时绝不执行abort，返回ReconciliationRequired。
- [x] RED：abort后只接受old HEAD、clean、无merge state/autostash且source ref不变；任何额外untracked/外部修改不reset/clean。
- [x] RED：MergePending分类区分old+clean未应用、exact expected commit已应用、无durable conflict receipt的conflict=reconciliation；AbortPending区分exact conflict可重试和old+clean已abort。
- [x] RED：Store reply lost只影响上层补写，不让runtime重新执行已证明成功的merge/abort。
- [x] GREEN：实现conflict observation digest、abort capability和按durable state分流的pure classifier。
- [x] REFACTOR：MergePending与AbortPending使用不同枚举/方法，禁止共享含糊布尔classifier。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test delivery_abort --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test delivery_recovery --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test delivery_merge --locked --offline
```

检查点：自动abort只处理应用可证明制造的现场，不会应用外部autostash。

## 任务 16：实现 non-force worktree unlock/remove 与分态恢复

**文件：**

- 新建 `crates/coding-agent-runtime/src/delivery/cleanup.rs`
- 新建 `crates/coding-agent-runtime/tests/delivery_cleanup.rs`
- 修改 `crates/coding-agent-runtime/src/delivery/{command,recovery,mod}.rs`

- [x] RED：只接受应用owned、fixed lock reason、clean、HEAD/source exact、无active/unknown process proof的source worktree；外部unlock/relock/admin/path/common identity drift拒绝。
- [x] RED：exact unlock后观察unlocked；remove命令不带force且只有RemovePending可执行。dirty新增文件导致known-not-applied并保留worktree。
- [x] RED：按durable phase分别恢复：`UnlockPending`只接受exact locked/present继续unlock或exact unlocked/present补写下一阶段；`UnlockedPendingRemove`只接受exact unlocked/present持久RemovePending。两阶段的path/admin absent、relocked、partial或identity mismatch一律reconciliation，不能猜测removed。
- [x] RED：只有`RemovePending`可在exact unlocked/present时重试non-force remove，或在path+admin都absent、authenticated common identity与source ref均exact时分类Removed；只缺一侧、common/source漂移或不可观察进入reconciliation。
- [x] GREEN：实现unlock/remove命令、fact observer和phase-specific recovery disposition。
- [x] REFACTOR：worktree cleanup不复用会force或猜测删除的P4-A reservation recovery helper。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test delivery_cleanup --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test delivery_recovery --locked --offline
cargo test -p coding-agent-runtime --test worktree --locked --offline
```

检查点：remove失败最多留下用户已确认的unlocked retained worktree，不覆盖新文件。

## 任务 17：实现 target-verified 原子 source branch 删除

**文件：**

- 修改 `crates/coding-agent-runtime/src/delivery/cleanup.rs`
- 新建 `crates/coding-agent-runtime/tests/delivery_branch_cleanup.rs`
- 修改 `crates/coding-agent-runtime/src/delivery/{command,recovery}.rs`

- [x] RED：delete前fresh证明worktree Removed、source ref=expected、未被其他worktree checkout、source object shape正确且source是fresh target HEAD ancestor。
- [x] RED：单个 `update-ref --stdin` transaction执行 target verify + source delete；检查后外部target reset或source drift使整批零删除失败。
- [x] RED：target合法前进且仍含source返回RefreshExpectedTarget，要求上层先持久新DeletePending version/head再重试；不沿用旧proof。
- [x] RED：source absent只有在fresh target ancestry与persisted expected source object shape同时精确成立时分类Deleted；source present但target不含source分类known-not-applied；source drift、object shape不符或source absent且任一proof不明分类reconciliation，不自动重建ref。
- [x] RED：命令不使用 `branch -D`、shell或单ref先删后查；non-UTF8/非法target ref在构造前拒绝。
- [x] GREEN：实现ancestry observer、atomic ref transaction builder和DeletePending-specific recovery disposition。
- [x] REFACTOR：ref transaction输入只接受validated ref/OID types，command stdout/parser有固定bounds。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --test delivery_branch_cleanup --locked --offline -- --nocapture
cargo test -p coding-agent-runtime --test delivery_cleanup --locked --offline
cargo test -p coding-agent-runtime --test delivery_security --locked --offline
```

检查点：应用不会因target TOCTOU删除唯一保留source commit的branch ref。

## 任务 18：完成 runtime 故障注入、安全矩阵与跨平台回归

**文件：**

- 修改 `crates/coding-agent-runtime/tests/support/mod.rs`
- 修改 `crates/coding-agent-runtime/tests/delivery_{source,source_commit,target,preflight,merge,abort,cleanup,branch_cleanup,security,recovery}.rs`
- 修改 `crates/coding-agent-runtime/src/{process_supervisor,lib}.rs`

- [x] RED：为每种Git child注入spawn前/后、stdout overflow、deadline、kill/wait/channel unknown和process-tree cleanup failure，锁定是否可重试、是否需reconciliation。
- [x] RED：Windows case/reparse/SUBST与Unix symlink/bind alias不能绕过common/admin/path identity；不同object format OID长度正确。
- [x] RED：恶意hook/filter/diff/merge driver/fsmonitor/signature helper/editor/askpass探针均保持零调用；环境和错误输出不泄露值/路径。
- [x] RED：长期重复preflight产生的仅为可接受dangling objects，不自动GC；temp index cleanup不删除非应用文件。
- [x] GREEN：补齐共享临时Git fixture、process fault controller、probe assertions和platform-specific capability tests。
- [x] REFACTOR：共享fixture不提供绕过真实command policy的shortcut；每个测试结束证明无存活child。
- [x] 验证：

```powershell
cargo test -p coding-agent-runtime --all-targets --locked --offline -- --nocapture
cargo clippy -p coding-agent-runtime --all-targets --locked --offline -- -D warnings
```

检查点：runtime只返回可证明事实，所有持久决策仍由app+StoreWriter完成。

## Checkpoint C：DeliveryManager、StoreWriter、repository lease 与恢复

## 任务 19：把 delivery typed transactions 接入唯一 StoreWriter

**文件：**

- 新建 `crates/coding-agent-app/src/store_writer/command/delivery.rs`
- 修改 `crates/coding-agent-app/src/store_writer/{command,command/execution,tests}.rs`
- 修改 `crates/coding-agent-app/src/store_writer.rs`
- 新建 `crates/coding-agent-app/tests/delivery_store_writer.rs`
- 修改 `crates/coding-agent-app/tests/store_writer.rs`

- [x] RED：为eligibility read以外的receipt、preflight/source/merge/conflict/disposition/cleanup current+journal写入提供exact typed command和receipt；禁止generic SQL closure。
- [x] RED：覆盖enqueue前/后、execute前/后、commit前/后、reply前、channel close、busy、rollback和writer degraded的KnownApplied/KnownNotApplied/OutcomeUnknown分类。
- [x] RED：query-first重放按 `(entity kind,id,version)` 和command receipt恢复；不能把reply lost当未执行或重复advance。
- [x] RED：StoreWriter command不获取repository lease、不调用DeliveryManager/TaskManager/Scheduler；delivery mutation不产生task event或dispatcher wake。
- [x] GREEN：实现delivery command enum/dispatcher/execution receipt，并把Store事务API封装到单一生产writer路径。
- [x] REFACTOR：按source、merge、cleanup拆分执行函数；主match只做路由，避免扩张现有大文件。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test delivery_store_writer --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test store_writer --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-store --test delivery_recovery --locked --offline
```

检查点：P4-B生产写入只有一个StoreWriter线性化入口。

## 任务 20：建立 DeliveryManager actor、资格查询与 bounded preflight worker

**文件：**

- 新建 `crates/coding-agent-app/src/delivery_manager/{mod,command,query,preflight}.rs`
- 新建 `crates/coding-agent-app/src/delivery_api_projection/{mod,dto,logical}.rs`
- 新建 `crates/coding-agent-app/tests/delivery_manager.rs`
- 修改 `crates/coding-agent-app/src/{repository_control,service_state,lib}.rs`
- 修改 `crates/coding-agent-app/tests/repository_control.rs`

- [x] RED：GET query先取一个Store eligibility snapshot，再结合TaskManager active ownership、process-tree proof和fresh runtime observation生成typed eligibility/reason/allowed actions；不靠message解析。
- [x] RED：preflight先做只读receipt query-first；首次请求等待global cap时不持lease，随后获取同common Git identity non-blocking lease，最后才同事务写receipt+PreflightPending。repository busy释放cap并返回stable retryable outcome，零receipt/operation副作用。
- [x] RED：全局Git-operation cap固定2；同repository由lease串行，不同repository最多2个并行；actor mailbox在worker/Store/Git慢时仍可响应query/shutdown。
- [x] RED：known-not-applied且无child/side-effect ownership时，可在持久pending diagnostic后验证释放lease/cap并bounded backoff，重试按cap->lease重新认证；outcome/cleanup unknown时不得释放。
- [x] RED：已有Pending/Ready/side-effect-active/Merged/reconciliation和idempotent replay遵守Store优先级；用户断开不取消durable accepted preflight。
- [x] RED：preflight每个fresh drift写Stale/Superseded/Rejected/Conflict/Ready准确状态；GET只查询，不自动重跑terminal或创建operation。
- [x] GREEN：实现actor command loop、bounded worker ownership、query projection和preflight orchestration，复用RepositoryControlCoordinator lease/poison。
- [x] REFACTOR：actor只管理调度/ownership；eligibility、lease acquisition、runtime call和Store transition分方法/文件。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test delivery_manager --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test repository_control --features test-support --locked --offline
cargo test -p coding-agent-app --test task_manager --features test-support --locked --offline
```

检查点：preflight是唯一在用户点击后创建的durable只读目标operation，GET和startup不代替用户点击。

## 任务 21：实现 accept -> source -> expected merge -> actual merge orchestration

**文件：**

- 新建 `crates/coding-agent-app/src/delivery_manager/{source,merge,abort,recovery}.rs`
- 新建 `crates/coding-agent-app/tests/delivery_merge.rs`
- 修改 `crates/coding-agent-app/src/delivery_manager/{mod,command}.rs`
- 修改 `crates/coding-agent-app/tests/delivery_manager.rs`

- [x] RED：merge POST先query-first replay；首次accept必须按global cap -> repository lease -> fresh operation/evidence/source/target/candidate-tree/ancestry/ignored-collision validation -> Store transaction写独立accept receipt和Ready->Accepted；durable Accepted后才返回202并驱动source。
- [x] RED：Accepted严格驱动 ObjectPending -> deterministic source object -> CommitPending -> real index/ref -> Committed；source pending known error保持Accepted并bounded backoff。
- [x] RED：source Committed后才构造/验证expected merge object，持久MergePending exact OID，再执行actual merge；任何步骤顺序倒置由pause gate测试失败。
- [x] RED：source commit与expected-object阶段可能耗时；actual merge child spawn前再次认证source committed exact OID/shape、source clean/fixed lock、target exact clean/branch/old HEAD/config、ancestry和ignored collision，不能沿用accept时的旧capability或观察。
- [x] RED：live actual merge返回known conflict时，先把child receipt、old HEAD、source、MERGE_HEAD、index stages、worktree digest和`MERGE_AUTOSTASH=absent` proof原子持久为`AbortPending`；Store未确认前绝不abort。随后exact abort与postcondition成功才写`Conflict`。
- [x] RED：MergePending live/recovery统一按可证明现场分流：old HEAD+clean+无merge state可重试exact merge；exact expected commit补写Merged+default disposition；known-zero-target-effect可Failed；无durable conflict receipt的conflict或其余unknown/mismatch写ReconciliationRequired并poison。
- [x] RED：AbortPending只允许exact persisted conflict重试abort或old HEAD+clean+无merge state补写Conflict；reply lost先查exact transition，proof漂移或autostash不可证明时reconcile+poison且不reset/clean。
- [x] RED：Store commit reply lost、HTTP disconnect、双击、restart和外部HEAD/config/file drift下side effect最多一次且operation可查询。
- [x] RED：same-repo reservation/admission与source/merge严格串行；既有已运行task不自动停止，后续交付面对新target head。
- [x] GREEN：实现source/merge/abort阶段编排、typed pending receipt query、lease lifecycle、poison和worker completion回传。
- [x] REFACTOR：source、merge与abort三个orchestrator文件；每个方法只做一阶段decision/transition，避免单个巨型pipeline方法。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test delivery_merge --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test delivery_manager --features test-support --locked --offline
cargo test -p coding-agent-app --test repository_control --features test-support --locked --offline
cargo test -p coding-agent-app --test artifact_reconciliation --features test-support --locked --offline
```

检查点：用户accept之前source/target ref不变，accept之后每个Git side effect都有先行durable intent。

## 任务 22：实现两步 cleanup orchestration 与 fresh ancestry refresh

**文件：**

- 新建 `crates/coding-agent-app/src/delivery_manager/cleanup.rs`
- 新建 `crates/coding-agent-app/tests/delivery_cleanup.rs`
- 修改 `crates/coding-agent-app/src/delivery_manager/{mod,command,recovery}.rs`

- [x] RED：cleanup先query-first replay；首次receipt也必须在global cap和repository lease之后写入。未Merged、source非Committed、worktree identity/clean/lock/process proof失败、active operation或poison时remove receipt零副作用拒绝。
- [x] RED：一个remove receipt自动驱动 UnlockPending -> UnlockedPendingRemove -> RemovePending -> Completed；崩溃在RetainedUnlocked后无需第二次用户确认。
- [x] RED：已知未应用remove留下`Failed + RetainedUnlocked`时，fresh新receipt在重新验证clean/identity/process proof后直接持久`RemovePending + RetainedUnlocked`并重试remove；app不得重新执行unlock。
- [x] RED：branch cleanup必须是第二个独立receipt，只在worktree Removed、source ref exact、无checkout、fresh target ancestry成立时进入DeletePending。
- [x] RED：target合法前进时先持久新expected target version/head再重试atomic ref transaction；target不再含source时Failed+Retained，source drift/unknown时reconcile+poison。
- [x] RED：remove/delete reply lost和双击不重复unlock/remove/delete；Failed fresh retry使用新receipt且不覆盖旧operation history。
- [x] GREEN：实现remove/delete acceptance、phase loop、fact transition、expected-target refresh和runtime outcome mapping。
- [x] REFACTOR：worktree和branch cleanup使用不同typed command/result；UI allowed-actions由fresh projection计算，不由按钮本地状态推断。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test delivery_cleanup --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test delivery_merge --features test-support --locked --offline
cargo test -p coding-agent-app --test repository_control --features test-support --locked --offline
```

检查点：默认永久保留；两个cleanup动作、receipt和确认边界始终独立。

## 任务 23：接入 delivery-aware artifact reconciliation 与完整冷启动顺序

**文件：**

- 新建 `crates/coding-agent-app/src/delivery_reconciliation.rs`
- 新建 `crates/coding-agent-app/src/runner_factory/delivery.rs`
- 新建 `crates/coding-agent-app/src/single_instance/start_primary/delivery.rs`
- 修改 `crates/coding-agent-app/src/{artifact_reconciliation,single_instance,bootstrap_join,lib}.rs`
- 修改 `crates/coding-agent-app/src/delivery_manager/recovery.rs`
- 修改 `crates/coding-agent-app/src/{runner_factory,single_instance/start_primary}.rs`
- 修改 `crates/coding-agent-app/src/single_instance/start_primary/{actors,http}.rs`
- 新建 `crates/coding-agent-app/tests/{delivery_recovery,delivery_startup}.rs`
- 修改 `crates/coding-agent-app/tests/{artifact_reconciliation,single_instance,event_dispatcher}.rs`

- [x] RED：启动保持P4-A完整顺序并插入已批准probe：lock/config/private paths -> held sentinel proof -> private Git capability probe -> DB history/v1-v5 -> coordinator -> ownership join -> non-owned Reserved reconciliation -> atomic P4-A recovery/high watermark -> EventDispatcher -> StoreWriter -> delivery recovery -> actors/bootstrap -> Web Ready。
- [x] RED：delivery-owned Committed、Merged、RetainedUnlocked、Removed、Deleted不进入P4-A base/worktree observer；非owned artifact继续原行为。
- [x] RED：Reserved/Inconsistent+delivery row、missing/attempt mismatch、Merged缺disposition、journal gap和无法界定common identity使startup fail closed，不回退猜测。
- [x] RED：按common identity和creation order恢复PreflightPending、Accepted/source pending、MergePending、AbortPending、active cleanup；只校正durable accepted operation，不创建新用户意图。
- [x] RED：每种runtime observation映射continue/applied/known failure/reconciliation+poison；Store reply lost只补写exact transition。
- [x] RED：P4-A recover_after_restart、committed high watermark、11 events、dispatcher cursor、Running=0和Scheduler bootstrap证据不漏失/重排。
- [x] GREEN：实现ownership router、delivery recovery coordinator和startup phase接线；Web Ready只在两套恢复全部闭合后开放。
- [x] REFACTOR：使用显式startup phase types，禁止在production StoreWriter启动后调用startup direct Store helper。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test delivery_startup --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test delivery_recovery --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test artifact_reconciliation --features test-support --locked --offline
cargo test -p coding-agent-app --test single_instance --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test event_dispatcher --features test-support --locked --offline
```

检查点：P4-B插入恢复阶段但不替换P4-A lifecycle/SSE冷启动不变量。

## 任务 24：完成 shutdown、degraded、process ownership 与 poison 隔离

**文件：**

- 新建 `crates/coding-agent-app/src/delivery_manager/shutdown.rs`
- 新建 `crates/coding-agent-app/src/shutdown/delivery.rs`
- 修改 `crates/coding-agent-app/src/{shutdown,service_state,repository_control,single_instance}.rs`
- 修改 `crates/coding-agent-app/src/shutdown/runtime.rs`
- 修改 `crates/coding-agent-app/src/single_instance/start_primary/actors.rs`
- 新建 `crates/coding-agent-app/tests/delivery_shutdown.rs`
- 修改 `crates/coding-agent-app/tests/{shutdown,degraded_recovery,single_instance}.rs`

- [x] RED：shutdown先关mutation gate和DeliveryManager intake，不再接受operation；已durable accepted worker完成当前可证明阶段或留下recoverable pending。
- [x] RED：等待全部Git child/process tree cleanup proof；outcome unknown时保留repository lease、worker ownership、global slot和primary lock，关闭HTTP且不有限安全退出。
- [x] RED：Store degraded但Git known applied时保留typed pending并query-first replay；Store/child双unknown不能降级为普通503/busy或释放poison。
- [x] RED：poison只隔离exact authenticated common identity；另一个repository仍可查询/运行，无法界定identity时冻结全部delivery mutation但不停止既有TaskManager safety cleanup。
- [x] RED：P4-A Scheduler/TaskManager quiesce、stop intent和process cleanup顺序不被DeliveryManager反向等待造成deadlock。
- [x] GREEN：把DeliveryManager纳入service quiesce barrier、shutdown join和single-instance failsafe，使用显式worker/child ownership receipt。
- [x] REFACTOR：delivery shutdown与TaskManager shutdown共享顶层相位，不共享含糊pending枚举或互相等待callback。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test delivery_shutdown --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test shutdown --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test degraded_recovery --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test single_instance --features test-support --locked --offline
```

检查点：任何未证明退出的Git child都不能因退出预算耗尽而释放single-instance安全边界。

## Checkpoint D：REST/OpenAPI、typed polling 与显式确认 UI

### HTTP status 锁定

本计划获批时一并锁定下列映射，实施中不得按message临时选择status：

- GET成功为200；task/operation不存在为404。
- preflight首次durable创建为201，同receipt重放为200 Existing。
- merge accept、remove worktree、delete branch首次durable接受为202，同receipt重放为200 Existing。
- malformed JSON/unknown fields为400，缺session为401，Origin/CSRF失败为403，错误media type为415，canonical/bounds字段校验为422。
- eligibility、stale/version、idempotency、operation-in-progress、target/source/cleanup前置条件冲突为409。
- 成功执行的preflight即使结果为`Conflict`仍返回201/200 operation projection；已202接受后出现的实际conflict通过GET 200投影。只有对Conflict operation发起非法accept才返回409。
- repository busy/poisoned、service degraded或reconciliation不可用为503；同步等待的Git command deadline为504；内部不变量破坏为500。
- 已durable accepted的后台operation后续timeout/failure通过operation GET投影，不把原202改写为另一个HTTP结果。

`retryable=true`只表示客户端可先重新查询、再安全重放同一receipt；它不授权自动生成新UUID或盲目重复POST。P4-B delivery-specific code的exact surface锁定如下；parse/session/Origin/CSRF/media-type/not-found/internal/store busy/degraded继续复用现有API code/status，不新造近义code：

| Stable code | 同步HTTP surface | `retryable` | durable operation后的surface |
| --- | --- | --- | --- |
| `TASK_NOT_MERGE_ELIGIBLE` | 409 | false | 不适用 |
| `DELIVERY_EVIDENCE_STALE` | 409 | false | GET 200中的`Stale` failure |
| `DELIVERY_SOURCE_CHANGED` | 409 | false | preflight阶段仅GET 200 `Stale`；Accepted且source为Absent/ObjectPending/CommitPending时绝不写Failed，known retryable error保持Accepted+pending，identity/fingerprint不一致改用`DELIVERY_SOURCE_INCONSISTENT`进入ReconciliationRequired |
| `DELIVERY_SOURCE_INCONSISTENT` | 503 | false | GET 200中的`ReconciliationRequired` failure |
| `DELIVERY_PREFLIGHT_STALE` | 409 | false | GET 200中的`Stale|Superseded` failure |
| `DELIVERY_OPERATION_IN_PROGRESS` | 409 | true | GET 200返回被引用的current operation |
| `TARGET_BRANCH_DETACHED` | 409 | false | GET 200 terminal failure（若已接受） |
| `TARGET_BRANCH_MISMATCH` | 409 | false | GET 200 terminal failure（若已接受） |
| `TARGET_HEAD_CHANGED` | 409 | false | GET 200中的`Stale|Failed` failure |
| `TARGET_WORKTREE_DIRTY` | 409 | false | GET 200 terminal failure（若已接受） |
| `TARGET_IGNORED_PATH_COLLISION` | 409 | false | GET 200 terminal failure（若已接受） |
| `TARGET_GIT_OPERATION_IN_PROGRESS` | 409 | true | GET 200 terminal failure（若已接受） |
| `UNSAFE_GIT_CONFIGURATION` | 409 | false | GET 200 terminal/reconciliation failure按durable state |
| `UNSUPPORTED_GIT_ATTRIBUTES` | 409 | false | GET 200 terminal/reconciliation failure按durable state |
| `MERGE_CONFLICT` | 对`Conflict`非法accept为409 | false | preflight创建仍为201/200，后台结果GET 200 `Conflict` |
| `SOURCE_ALREADY_IN_TARGET` | 409 | false | GET 200 terminal failure（若已接受） |
| `DELIVERY_RECONCILIATION_REQUIRED` | 503 | false | GET 200 `ReconciliationRequired`，不得把GET改成503 |
| `ARTIFACT_CLEANUP_NOT_ALLOWED` | 409 | false | 不适用 |
| `ARTIFACT_PROCESS_STILL_ACTIVE` | 409 | true | 不适用 |
| `WORKTREE_IDENTITY_MISMATCH` | 409 | false | GET 200 `ReconciliationRequired`（若已接受） |
| `SOURCE_BRANCH_NOT_MERGED` | 409 | false | GET 200 `Failed + Retained`（若已接受） |
| `IDEMPOTENCY_CONFLICT` | 409 | false | 不适用 |
| `REPOSITORY_CONTROL_BUSY` | 503 | true | 不创建operation/receipt |
| `REPOSITORY_CONTROL_POISONED` | 503 | false | 已有operation仍可GET 200查询 |
| `COMMAND_TIMED_OUT` | durable acceptance前为504 | true | acceptance后只在GET 200 failure/pending projection中出现 |
| `PROCESS_TREE_CLEANUP_FAILED` | 503 | false | GET 200 `ReconciliationRequired`且持有poison/ownership |

## 任务 25：原子增加 Delivery REST contract、backend、security 与 OpenAPI

**文件：**

- 新建 `crates/coding-agent-api/src/contract/delivery_contract.rs`
- 新建 `crates/coding-agent-api/src/delivery_wire/{mod,validation}.rs`
- 新建 `crates/coding-agent-api/src/router/delivery.rs`
- 新建 `crates/coding-agent-api/src/backend/delivery.rs`
- 新建 `crates/coding-agent-api/src/error/delivery.rs`
- 修改 `crates/coding-agent-api/src/{contract,backend,router,error,lib}.rs`
- 新建 `crates/coding-agent-api/tests/delivery.rs`
- 修改 `crates/coding-agent-api/tests/{openapi,router,sse,support/mod}.rs`
- 新建 `crates/coding-agent-app/src/server/delivery/{mod,projection,error}.rs`
- 修改 `crates/coding-agent-app/src/{server,service_state}.rs`
- 新建 `crates/coding-agent-app/tests/delivery_server.rs`
- 修改 `crates/coding-agent-app/tests/{server,security}.rs`
- 修改 `web/openapi.json` 与 `web/src/api/generated/schema.d.ts`

- [x] RED：锁定两个GET、preflight、merge accept、remove worktree、delete branch的exact request/response、required fields、deny unknown、UUID/OID/ref/fingerprint/version/conflict bounds和HTTP status。
- [x] RED：API把validated typed command交给Store共享canonical hash实现，不在router复制算法；逐字段变化、跨action UUID复用和JSON顺序/空白等价向量与Store tests一致。
- [x] RED：所有POST覆盖session、exact loopback Origin、CSRF、content type、mutation gate Ready和bounded body；GET仍需session。
- [x] RED：handler只等待command receipt/durable acceptance，不获取repository lease、不执行Git、不等待完整merge/cleanup；HTTP断开不取消operation。
- [x] RED：stable errors逐项映射status/code/message/retryable/details；响应不含prompt、diff、绝对路径、stderr、环境/config值或conflict内容。
- [x] RED：operation GET为discriminated exact DTO并按version投影；task delivery GET包含eligibility/evidence summary、fresh target observation `{available, branch, head, reason}`、source/latest merge/disposition/allowed actions，供Web构造preflight的exact target branch/HEAD；观察不可用时禁用action且不接受客户端猜测。
- [x] RED：OpenAPI path/schema/enum/maxLength/maxItems/status精确；SSE `oneOf`、11种task event和cursor tests逐字节不变。
- [x] GREEN：实现独立Delivery backend port、短router委托、app projection/error mapping和六条route；同一变更导出OpenAPI并生成TypeScript。
- [x] REFACTOR：delivery contract/backend/router/error各自子模块，禁止继续堆进现有大文件；API DTO不导出Store/runtime内部identity。
- [x] 验证：

```powershell
cargo test -p coding-agent-api --test delivery --locked --offline -- --nocapture
cargo test -p coding-agent-api --test openapi --locked --offline
cargo test -p coding-agent-api --test router --locked --offline
cargo test -p coding-agent-api --test sse --locked --offline
cargo test -p coding-agent-app --test delivery_server --features test-support --locked --offline -- --nocapture
cargo test -p coding-agent-app --test security --features test-support --locked --offline
cargo run --locked --offline -p coding-agent-api --bin export_openapi -- web/openapi.json
npm --prefix web run api:generate
npm --prefix web run api:check
```

检查点：P4-B只增加REST polling contract，不增加或复用task lifecycle event。

## 任务 26：实现 Web exact validator、client 与 polling reducer

**文件：**

- 新建 `web/src/api/{authenticatedTransport,authenticatedTransport.test,deliveryClient,deliveryClient.test,deliveryValidation,deliveryValidation.test}.ts`
- 新建 `web/src/state/{deliveryModel,deliveryReducer,deliveryReducer.test,useDeliveryPolling,useDeliveryPolling.test}.ts`
- 修改 `web/src/api/{client,client.test,types}.ts`
- 修改 `web/src/main.tsx`

- [x] RED：exact validator拒绝未知/缺失/null/unsafe integer、非canonical UUID/OID/ref/fingerprint、非法state组合和oversized conflict payload。
- [x] RED：四个POST各自生成/持有独立client request ID；网络结果未知时同action重试复用原ID，不同action或用户fresh retry使用新ID。
- [x] RED：reload/task切换先GET delivery；durable accepted后按operation ID/version polling，500ms起、最多2s，version前进重置退避，terminal停止并刷新delivery projection。
- [x] RED：unmount/task切换abort旧请求并忽略迟到response；旧operation不得覆盖新task/newer version，stale modal清空。
- [x] RED：polling/reducer不订阅或伪造SSE delivery event，不修改`applied_task_event_id`、membership cursor或scheduler snapshot。
- [x] RED：existing ApiClient与DeliveryClient共用唯一authenticated transport；session cookie/CSRF、401/session-expiry、request ID、abort、JSON/error envelope和network error语义逐项一致，禁止复制私有`#request`逻辑。
- [x] GREEN：从现有ApiClient抽取共享authenticated transport，实现独立delivery typed facade、pure reducer和effect hook；Agent lifecycle state与Delivery controller分离注入组件。
- [x] REFACTOR：不继续扩大通用`validation.ts`/`useAgentState.ts`；timer、network、validation和pure state各自单职责。
- [x] 验证：

```powershell
npm --prefix web run test:run -- src/api/authenticatedTransport.test.ts src/api/client.test.ts src/api/deliveryClient.test.ts src/api/deliveryValidation.test.ts src/state/deliveryReducer.test.ts src/state/useDeliveryPolling.test.ts
npm --prefix web run api:check
npm --prefix web run typecheck
```

检查点：浏览器刷新、断线和reply lost都从服务端durable projection恢复，而不是从按钮本地状态猜测。

## 任务 27：实现 Delivery panel、preflight/merge确认与两个cleanup modal

**文件：**

- 新建 `web/src/components/DeliveryPanel/{index,Eligibility,PreflightModal,MergeProgress,CleanupControls}.tsx`
- 新建 `web/src/components/DeliveryPanel.test.tsx`
- 修改 `web/src/components/{TaskWorkspace,TaskWorkspace.test,AppShell,AppShell.test}.tsx`
- 修改 `web/src/styles.css`

- [x] RED：ineligible只显示稳定原因且没有可点击Merge；eligible显示服务端target branch/HEAD和evidence摘要，不显示绝对路径/diff。
- [x] RED：preflight modal展示exact generation/fingerprint/source/target和clean/conflict结果；Merge二次确认只能提交同一Ready operation/version，stale modal禁用。
- [x] RED：accepted/pending按operation projection禁用重复动作；terminal/reload恢复，Conflict只显示bounded relative path summary和“重新预检”，不提供自动编辑。
- [x] RED：Merged后默认明确显示worktree/branch保留；Remove worktree与Delete branch是两个按钮、两个dialog、两个receipt，branch只在allowed action出现。
- [x] RED：覆盖keyboard、focus return、Escape、ARIA dialog/labels、long OID/ref/path wrapping和loading/error/retry状态。
- [x] GREEN：实现最小可访问panel和modal，所有allowed action来自typed server projection，组件不解析message或Git文本。
- [x] REFACTOR：TaskWorkspace只装配panel；展示、preflight、progress、cleanup分组件，modal共享仅限可访问性primitive，不合并两个cleanup语义。
- [x] 验证：

```powershell
npm --prefix web run test:run -- src/components/DeliveryPanel.test.tsx src/components/TaskWorkspace.test.tsx src/components/AppShell.test.tsx
npm --prefix web run config:check
npm --prefix web run typecheck
npm --prefix web run build
```

检查点：UI明确表达“用户显式本地交付”，不宣称远程发布、生产安全或全局Git锁。

## Checkpoint E：离线系统证明、文档与最终验收

## 任务 28：实现 Rust/API 离线 delivery E2E 与 crash/race 矩阵

**文件：**

- 新建 `crates/coding-agent-app/src/test_support/delivery.rs`
- 修改 `crates/coding-agent-app/src/test_support.rs`
- 新建 `crates/coding-agent-app/tests/{controlled_delivery_offline_e2e,delivery_crash_e2e}.rs`
- 修改 `crates/coding-agent-app/tests/{process_support,offline_e2e,multi_role_offline_e2e,concurrent_offline_e2e}.rs`
- 修改 `crates/coding-agent-app/tests/support/mod.rs`

- [x] RED：临时真实Git/Cargo仓库+scripted provider证明Approved task在点击前无source commit/merge，显式accept后得到exact no-ff双parent commit和clean target。
- [x] RED：historical Completed+Unreviewed、Rejected、非Completed、dirty/detached/stale target、ignored collision和source fingerprint drift全部拒绝且用户bytes/ref不变。
- [x] RED：clean/conflict preflight目标byte-for-byte不变；外部修复/推进后新preflight成功，source已在target ancestor稳定拒绝。
- [x] RED：source ObjectPending/CommitPending、MergePending/AbortPending、unlock/remove/delete每个关键pause/crash/reply-lost点重启收敛且side effect最多一次。
- [x] RED：同repo merge/reservation/cleanup串行、不同repo最多2并行、poison隔离、shutdown unknown child持有primary lock和delivery ownership。
- [x] RED：完整startup ownership overlay证明Committed/Removed/Deleted不被P4-A误判，P4-A high watermark/11 events/Scheduler bootstrap保持。
- [x] GREEN：扩展deny-unknown strict ProcessTestConfig、delivery fault/pause/release和真实Git assertions；不增加生产测试后门。
- [x] REFACTOR：Git fixture、crash controller、receipt replay和process cleanup assertions放共享test support；测试结束证明无child/held sentinel/temp leak。
- [x] 验证：

```powershell
cargo test -p coding-agent-app --test controlled_delivery_offline_e2e --all-features --locked --offline -- --nocapture
cargo test -p coding-agent-app --test delivery_crash_e2e --all-features --locked --offline -- --nocapture
cargo test -p coding-agent-app --test process_support --features test-support --locked --offline
cargo test -p coding-agent-app --test concurrent_offline_e2e --all-features --locked --offline
cargo test -p coding-agent-api --test delivery --locked --offline
```

检查点：所有系统证明离线、可重复且不联系真实provider或remote Git。

## 任务 29：实现 Playwright delivery/recovery 场景并更新用户文档

**文件：**

- 新建 `web/e2e/{controlled-delivery,delivery-recovery}.spec.ts`
- 新建 `web/e2e/support/delivery.ts`
- 修改 `web/e2e/support/localApp.ts`
- 修改 `web/e2e/{core-workflows,fault-recovery,lifecycle,security,startup-order,ui-edge-cases}.spec.ts`
- 修改 `README.md`

- [x] RED：浏览器证明未点击不merge、preflight详情/二次确认、pending polling/reload、conflict无目标修改、stale后重新preflight和成功no-ff merge。
- [x] RED：证明两个cleanup modal/receipt独立；remove后branch默认Retained，delete后Deleted，reply lost/double click不重复副作用。
- [x] RED：安全场景覆盖session/Origin/CSRF、unknown fields、脱敏errors、恶意config、ignored collision和绝对路径不显示。
- [x] RED：强杀primary覆盖source/merge/abort/cleanup pending，重启先recovery后Ready；network guard证明零remote/provider流量。
- [x] GREEN：实现typed localApp delivery helper和最小Playwright场景；只使用execFile/临时本地repo，不用shell拼Git命令。
- [x] GREEN：README记录资格、current checkout目标、固定no-ff、默认保留、冲突边界、两步cleanup、恢复/poison和“非OS/全局Git锁”。
- [x] REFACTOR：UI driver、Git断言、crash orchestration分离；teardown等待所有进程并清理临时目录。
- [x] 验证：

```powershell
cargo build --locked --offline -p coding-agent-app --features e2e
$env:CODING_AGENT_E2E_BINARY = (Resolve-Path '.\target\debug\coding-agent-app.exe').Path
try {
  npm --prefix web run e2e -- controlled-delivery.spec.ts delivery-recovery.spec.ts security.spec.ts startup-order.spec.ts
} finally {
  Remove-Item Env:CODING_AGENT_E2E_BINARY -ErrorAction SilentlyContinue
}
```

macOS/Linux将环境变量设置替换为：

```bash
CODING_AGENT_E2E_BINARY="$PWD/target/debug/coding-agent-app" npm --prefix web run e2e -- controlled-delivery.spec.ts delivery-recovery.spec.ts security.spec.ts startup-order.spec.ts
```

检查点：用户文档和演示都明确“本地、显式、可恢复、默认保留”。

## 任务 30：独立代码审查与完整验收

- [x] 并行至少三路独立审查：
  - Store/迁移/receipt/事务：v5、evidence/directory identity、state/nullability、journal、idempotency、11-event兼容。
  - Runtime/Git安全：exact stdin、command policy、source/merge OID、ignored/config/attributes、abort、atomic ref transaction、process proof。
  - App/API/Web：lease/cap、startup/shutdown/poison、HTTP/security/OpenAPI、polling/UI/E2E和范围边界。
- [x] 解决全部 Blocker/High findings；每次修复先补RED或运行已有失败测试，再运行聚焦和完整门禁。
- [x] 人工逐条核对最终diff：无第7种TaskStatus、无第12种event、无SSE delivery、无auto merge/cleanup、无remote/PR、无P4-C/P4-D、无大文件/大方法回归。
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
$env:CODING_AGENT_RELEASE_BINARY = (Resolve-Path '.\target\release\coding-agent-app.exe').Path
try {
  cargo test --locked --offline -p coding-agent-app --test release_smoke --features embedded-web -- --ignored --exact release_binary_starts_without_node_or_dist
} finally {
  Remove-Item Env:CODING_AGENT_RELEASE_BINARY -ErrorAction SilentlyContinue
}

node scripts/check-placeholders.mjs
git diff --check
```

- [ ] 在承载最终候选commit的现有CI上，`quality-e2e`（Ubuntu）与`release-smoke`矩阵的`ubuntu-latest`、`windows-2022`、`macos-latest`全部成功；核对的workflow必须仍执行P4-B相关workspace/Web/E2E或明确的等价门禁。
- [ ] 上述PowerShell命令只算Windows本地证据；CI未运行、被跳过或任一平台失败时，状态只能报告“本地部分验证”，不得报告P4-B完成。

## 完成定义

只有以下条件全部满足，P4-B 才可报告完成：

- 任务1–29均有先失败、后通过的聚焦测试证据，任务30完整门禁使用最终代码全绿。
- 最终候选commit的现有`quality-e2e`与Linux/Windows/macOS三平台`release-smoke`全绿；不能用单平台本地结果替代。
- 独立审查Blocker/High为0，最终diff与已批准规格/计划逐条一致。
- 未点击不创建source commit/merge/cleanup；显式merge产生exact no-ff commit，冲突不改目标。
- source/merge/abort/cleanup每个crash/reply-lost/unknown路径均收敛到可证明状态或reconciliation+poison。
- worktree与branch默认保留，cleanup始终是两个独立用户动作且branch delete具有原子target verify。
- 六态Task、readiness/evidence、artifact事实、11种persisted event、SSE cursor和P4-A startup/Scheduler不变量保持。
- 没有真实provider/remote Git尝试，没有rebase/push/PR/自动cleanup，没有开始P4-C/P4-D。

## 当前门禁

本计划已获用户书面批准，P4-B 实施授权生效。必须按任务 1–30 顺序推进；未完成任务 30 的独立审查、跨平台 CI 与完整门禁前，不得报告 P4-B 或 Project 4 完成。
