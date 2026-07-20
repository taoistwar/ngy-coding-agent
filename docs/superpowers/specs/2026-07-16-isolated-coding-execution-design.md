# Project 2：隔离式 Coding 执行设计

> 日期：2026-07-16
> 状态：初稿，等待书面规格复核
> 前置条件：Project 1 已完成并验收

## 1. 目标

Project 2 用真实 `CodingAgentRunner` 替换生产 composition root 中的确定性
`FakeTaskRunner`。一个任务必须在独立 Git worktree 中完成最小闭环：理解用户请求、读取与搜索代码、修改文件、运行 Rust 测试，并把计划、活动、差异和测试结果持续写入 Project 1 的持久事件流。

本项目解决“安全且可观察地执行代码修改”，不解决 Planner/Executor/Reviewer 多角色质量闭环，也不提供合并能力。`TaskStatus::Completed` 仍只表示 runner 正常结束，默认不代表可交付、已审查或可合并。

## 2. 范围

### 2.1 包含

- 每次 task attempt 创建唯一任务分支和独立 Git worktree；
- 文件列表、文件读取、文本搜索、单文件安全替换与受控 Cargo/Git 操作；
- Rust/Cargo workspace 感知以及测试命令的结构化结果；
- OpenAI-compatible provider 的一个明确兼容子集；
- 单角色模型循环及结构化工具调用；
- 命令超时、任务取消、输出上限和子进程树清理；
- worktree 路径约束、环境变量白名单、凭据与日志脱敏；
- diff/test/activity/plan 通过既有 `RunnerEventSink` 持久化；
- 本地 mock HTTP provider 驱动的离线端到端测试；
- 生产真实 runner 全局并发固定为 1。

### 2.2 不包含

- Planner、Executor、Reviewer 三角色编排和返工循环；
- reviewer approval、delivery readiness 或自动合并；
- 同仓库多个真实任务并发、资源配额和 worktree 回收策略；
- 跨文件事务式修改；
- shell 任意继承宿主环境或对 worktree 外路径的写入授权；
- provider 自动发现、多个 provider profile、OAuth 或云端密钥托管；
- 安装器、签名、自动更新和发行包装。

### 2.3 威胁模型与“隔离”的含义

Project 2 的“隔离”是 Git 工作区隔离和第一方工具 capability 隔离，不是针对恶意代码的 OS 沙箱。应用信任用户主动登记的仓库，以及模型生成后由 Cargo/build/test 执行的代码；这些代码都会获得当前 OS 用户权限。应用不把模型输出、工具参数、provider 响应或仓库中用于诱导模型的文本当作工具协议、路径或控制面授权，但一旦模型写入的代码被允许进入 Cargo 执行，它属于本节明确的受信执行代码。

`cargo check/test` 可能执行原有或模型生成的 `build.rs`、proc-macro、测试二进制和依赖代码。这些进程仍以当前 OS 用户权限运行，能够尝试读写 worktree 外路径或联网；`cwd`、`env_clear()`、Unix process group 和 Windows Job Object 都不能把它们变成文件系统/网络沙箱。Project 2 负责减少工具协议层的意外越界、隔离敏感环境变量并可靠终止进程树，但不宣称安全执行恶意或失控代码。真正不信任代码的强沙箱需要另立规格，覆盖各平台文件系统、网络、CPU、内存、进程数和磁盘配额。

## 3. 不变量

1. 用户原始工作目录只用于 Git 元数据操作，不读取其中未提交文件作为模型上下文，也不修改或清理它。
2. Agent 的第一方文件工具目标和命令 `cwd` 只能位于当前 attempt 的 worktree 内；被执行的仓库代码不受此路径声明约束，边界以 2.3 的威胁模型为准。
3. worktree 基于仓库当前已提交 `HEAD` 创建；原目录中的 staged、unstaged 和 untracked 内容不会进入任务。
4. 每个 attempt 使用新的分支与 worktree；retry 不复用旧 attempt 的可写目录。
5. 文件工具使用根目录 capability 与逐段 no-follow 打开，不依赖可竞态的“canonicalize 后按字符串路径操作”；拒绝绝对路径、`..`、`.git`、symlink/junction/reparse point、Windows alternate data stream 和平台等价路径逃逸。
6. 模型不能调用控制面动作；完成、失败和取消由 runner 编排器解释。
7. 模型文本和工具参数都视为不可信输入。
8. 测试结果只描述实际运行过的命令；未运行测试不得显示为 passed。
9. 取消或超时后必须终止整棵子进程树，再返回 terminal outcome。
10. Project 1 的 REST、SSE、Task 生命周期和既有 DTO 语义保持兼容。

## 4. crate 与依赖方向

新增三个 workspace crate：

- `coding-agent-runtime`：Git worktree、受限文件工具、patch/replace、进程执行、Cargo 发现、diff 和测试采集；
- `coding-agent-provider`：OpenAI-compatible HTTP 协议、请求/响应 DTO、流式或非流式传输、错误归一化和脱敏；
- `coding-agent-core`：单角色循环、工具 schema、上下文预算、停止条件以及从工具结果到 runner 事件的编排。

依赖方向固定为：

```text
app --> domain
app --> store
app --> api
app --> core
app --> provider --> core
app --> runtime  --> core
```

箭头表示“依赖”。`core` 定义 provider/runtime ports 和中性事件，不依赖 `app`；`provider` 与 `runtime` 实现这些 ports；`app` 在 composition root 组装具体实现、把中性事件映射为 domain `RunnerEvent`，并提供 `CodingAgentRunner`。现有 `TaskRunner`/`RunnerEventSink` 继续留在 `app`，因此不会形成 `app <-> core` 循环。`domain`、`store`、`api` 不依赖 Git、进程或 provider。

## 5. attempt 与 worktree 生命周期

### 5.1 命名和位置

- 分支名：`codex/task-<task-id>-attempt-<attempt>`；
- worktree 位于应用私有数据目录的 `worktrees/<repository-id>/<task-id>/<attempt>`；
- 分支和目录都由应用生成，不接受模型提供的名称；
- 创建前验证目标目录不存在，分支不存在；冲突返回稳定错误而不覆盖旧制品。

### 5.2 创建

runner 启动后按顺序执行：

1. 验证登记的 Git root 和 Cargo workspace root 仍存在且属于同一仓库；
2. 读取 Git root 当前已提交 `HEAD`；若无 commit，任务失败；
3. 先返回确定性的 reservation（base/branch/path/source identity/workspace offset）供应用持久化为 `reserved`，之后才允许 Git side effect；
4. 使用参数数组而非 shell 字符串执行 `worktree add --no-checkout`；禁用 hooks，并在 checkout 前检测并拒绝 filter、include/includeIf、per-worktree config 等可改变或执行 checkout 行为的配置；
5. 只从 retained common Git directory 的 admin metadata 认证 linked git-dir，再以固定 `reset --hard --no-recurse-submodules <base>` materialize；
6. 在新 worktree 中重新发现 Cargo workspace，以 handle-relative plain `target` capability 运行可信 `cargo metadata` 并证明 exact workspace root；
7. 写入 activity，并发布初始 plan；
8. 启动模型/工具循环。

Git 调用使用应用固定的可信 work-tree/git-dir，不采用模型可改写的路径；后续 diff/status 禁用 external diff、textconv、fsmonitor 和 hooks。worktree 根部的 `.git` 指针是受保护元数据，永远不作为模型可读写文件暴露。

最终 diff 以 `status --porcelain=v2 -z --no-renames` 的原始路径字节作为有界输入，按原始字节序确定性排序，再逐文件用固定 literal pathspec 分离采集 `numstat` 与 patch：计数必须来自完整未截断的 machine output，patch 只保留真实前缀并按 UTF-8 字节上限标记截断。patch 固定 `core.quotePath=true`、`--no-color`、`--no-ext-diff` 和 `--no-textconv`；binary 只发布有界元数据。非 ignored untracked 文件从 retained worktree capability 逐段 no-follow 打开，受单文件与累计字节上限约束；非 UTF-8 路径使用无碰撞的百分号展示。采集前后 status 不一致、任何 machine output 不完整、路径/文件数/内容上限触发或 capability 身份变化都 fail closed；linked-worktree `.git` 文件从不参与授权。

创建由持久 artifact 状态机协调。先持久化 `reserved`，再执行 Git side effect，验证后持久化 `ready`。如果创建过程部分失败，runner 只清理由本次尚未进入 `ready` 且身份完全匹配的临时目录/引用；任何身份不一致都标记 `inconsistent` 并保留现场，不做猜测性删除。

从首次 Git side effect 开始，任一 spawn/wait/cancel/timeout/admin/reset/status/worktree-list/Cargo 验证错误都必须在返回前执行只读 observation。observation 只从 persisted reservation、common-side admin metadata、固定 branch/base、validated target capability、clean status 与 exact Cargo metadata 分类 `absent|branch-only|administrative-created|checkout-partial|ready|inconsistent`，不读取 linked worktree `.git` 内容作为 authority，也不删除或猜测性修复现场。错误同时保留原始 cause 与 observation；即使原 cancellation token 已触发，分类也使用新的有界只读 token 完成。

### 5.3 保留

Project 2 对 Completed、Failed、Cancelled、Interrupted 的 worktree 和分支一律保留，供用户检查。清理和配额属于 Project 4。应用重启不自动继续旧进程；Project 1 的恢复规则仍把运行中任务标记为 Interrupted。

## 6. Repository runtime ports

所有路径参数使用相对 worktree 根的 UTF-8 slash path。解析后任何分量为平台等价的 `.git`、`.`、`..`，或包含 NUL/Windows ADS 语法时一律拒绝。非 UTF-8 路径可以出现在 Git diff 展示中，但 Project 2 的模型文件工具不操作它，并返回稳定错误。

路径解析只产生逻辑 `RelativePath`；实际访问必须从预先打开并验证的 worktree root handle/fd 出发逐段 no-follow 打开，最终读写与 rename 使用 handle-relative API，并在最终 handle 上验证文件类型和身份。静态 prefix/canonical 检查只能作为附加诊断，不能作为安全授权。若目标平台无法提供无竞态语义，对应写工具必须 fail closed。

### 6.1 工具集合

- `list_files { path, depth, limit }`
  - 有界列出目录；强制隐藏所有 `.git` 元数据，并默认忽略常见构建目录；
- `read_file { path, start_line, end_line }`
  - 文本读取，带行号、单次字节上限和截断标记；拒绝二进制文件；
- `search_text { query, path, glob, limit }`
  - 字面量搜索；首选内置遍历，不能依赖用户机器预装 `rg`；
- `replace_file { path, expected_sha256, content }`
  - 单文件安全替换；以读取时 digest 做乐观并发检查；
  - `expected_sha256=null` 表示目标必须不存在；字符串表示目标必须存在且 digest 精确匹配；
  - 同目录临时文件写入、flush、权限保留、原子 rename/replace；
  - Windows 被占用等无法原子替换时失败，不降级为截断覆盖；
- `cargo_check { package, timeout_ms }`、`cargo_test { package, test, timeout_ms }`
  - package/test 必须来自本次可信 `cargo metadata` 发现集；内部生成封闭参数并返回结构化摘要；
- `git_status {}`、`git_diff {}`
  - 只接受无路径参数的 typed 调用，由 runtime 绑定可信 work-tree/git-dir 并生成固定参数。

模型没有通用 `run_command`、读取/修改 `.git`、删除文件、移动文件、Git commit、Git push、网络下载或 shell 工具。模型不能注入 `-C`、`--git-dir`、`--work-tree`、`--no-index`、`--manifest-path`、`--target-dir`、`--config` 等参数。新增文件通过 `replace_file` 在父目录 capability 验证后完成。删除/移动和通用命令能力留待后续规格扩展。

单文件替换只承诺一次原子 namespace publication，不承诺跨进程线性 CAS 或跨崩溃事务。实现使用同目录独占临时文件、`sync_all`、发布前重验与平台原子 API；只保留 POSIX mode/Windows readonly 等明确元数据。Unix 会在发布前以 dev/inode/type 重验临时目录项，但可移植 API 没有 rename-by-fd，因此与同一 OS 用户权限、在最后重验之后继续恶意改写该目录项的并发 actor 不存在线性化保证；这类 actor 按 2.3 的受信宿主代码边界处理。原子发布成功后，即使随后观察到 cancellation，该修改也视为 committed 并进入 diff。

### 6.2 读取与输出上限

- 单文件单次读取默认 256 KiB；
- 单次目录/搜索结果默认 200 项，硬上限 1000 项；
- 单次命令 stdout、stderr 各保留最多 1 MiB；
- 超限后继续排空 pipe 直到进程结束，避免子进程阻塞，但只持久化前后摘要和 `truncated=true`；
- activity 单条消息和 provider tool result 都有独立上限，禁止把完整巨量输出写入 SQLite 或模型上下文。

具体数值集中在 `RuntimeLimits`，测试可以注入更小值，生产值不能由模型提高。

## 7. 命令执行与取消

内部命令由不可由模型构造的 `ValidatedCommand` 表示，不调用 `sh -c`、`cmd /C` 或 PowerShell。应用启动时解析、验证并固定 Git/Cargo/Rust 工具的绝对可执行文件身份，不按模型输入或 child cwd 搜索 PATH。发现阶段通过同一个有界 supervisor 执行固定的 `git --version` 探针，并要求 Git 2.45 或更新版本；这是固定 `--no-lazy-fetch` 防护参数的明确能力下限，旧版本在启动时以稳定错误 fail closed。模型只看到第 6.1 节 typed 工具；runtime 为 Git status/diff 和 Cargo metadata/check/test 生成固定 argv，不提供任意 program/args/cwd 接口。

固定身份在 spawn 边界继续保持：Unix 从已打开的 executable fd 经 `/proc/self/fd` 或 `/dev/fd` 执行，并在 `pre_exec` 中对已打开的 worktree fd 执行 `fchdir`；Windows 对 executable 持有 deny-write/deny-delete lease，并在 suspended `CreateProcess` 消费 cwd 路径期间持有已重验身份的目录 lease。Cargo 的 `RUSTC`、`RUSTDOC` 与可离线调用的 Git 都来自同一组 pinned tool handles，且在每次 Cargo spawn 前一并重验。直接或未知来源的 bootstrap `rustc --print sysroot` 也通过同一个 supervisor，而不是另走只终止 leader 的临时进程路径；对于严格 canonical、逐级 containment 校验且与 rustup 默认 concrete compiler 完全匹配的 `<RUSTUP_HOME>/toolchains/<default>`，直接采用该受管根作为 sysroot，避免 macOS `/dev/fd` 执行破坏 compiler 自定位。

Cargo metadata 只有在完整未截断时才解析；workspace root、固定的 worktree 内 `target` 目录、每个 workspace member manifest 与 target source 都必须再次通过 worktree handle no-follow 打开验证。package 与 integration-test selector 只接受这次 metadata 的精确发现值，check/test 的总 timeout 同时覆盖前置 metadata 与最终命令。

每个进程具备：

- 独立 stdout/stderr pipe 和并发排空；
- wall-clock timeout；
- 与 task cancellation token 联动；
- Windows 在目标进程首条用户代码运行前加入带 `KILL_ON_JOB_CLOSE` 的 Job Object；Unix 使用独立 process group；
- terminal 前的进程树 kill + bounded wait；
- `CommandResult { exit_code, signal, timed_out, cancelled, stdout, stderr, truncated, duration_ms }`。

Windows 实现不能采用普通 spawn 后再 `AssignProcessToJobObject` 的可逃逸窗口；应使用 `STARTUPINFOEX` 的 job-list attribute，或 suspended create → assign → resume，并测试孙进程。Unix process group 无法约束主动 `setsid()` 的恶意代码，这属于 2.3 中明确排除的强沙箱边界。

Unix supervisor 以 `waitid(..., WNOWAIT)` 保留 leader PID/PGID 锚，并让正常子孙继承一个只用于终态确认的 liveness writer；group kill 后先等待 reader EOF，再 reap leader。Darwin 的可移植 `pipe()` + `fcntl(FD_CLOEXEC)` 回退由应用级共享 spawn lock 覆盖，所有已知生产 spawn 都通过同一协调点。主动 `setsid`、`close_range`/`closefrom` 或显式关闭/改写该内部 fd 都属于 2.3 中已排除的恶意或主动逃逸代码边界，Project 2 不把这一机制表述为 OS 沙箱或对任意进程的内核级成员证明。

即使 leader 正常退出，只要其孙进程仍存活或仍持有 stdout/stderr pipe，supervisor 也先清理进程树，再完成有界 pipe drain；supervisor future 被 abort/drop 时由 RAII guard 触发同样的 kill。`cancelled` 优先于同时可观察到的 timeout，测试锁定该竞态规则。

取消优先于后续工具调用。若取消发生在安全文件替换的最终 rename 前，删除临时文件；rename 已成功则保留已完成的单文件修改并在最终 diff 中可见。

## 8. 环境与网络边界

子进程先 `env_clear()`，再加入最小白名单：平台必需的系统路径变量、临时目录、locale，以及显式允许的 Cargo/Rust 工具链变量。默认不转发 token、cookie、云凭据、SSH agent、Git askpass、代理、CI secrets 或用户自定义环境变量。

provider 凭据只由应用进程用于 HTTP Authorization，不进入模型消息、工具结果或子进程环境。日志记录 endpoint origin、状态码和 request id，但不记录 Authorization、完整请求 body、完整模型响应或疑似 secret。统一脱敏器覆盖 tracing、activity 和用户可见失败消息。

Project 2 的 Cargo 命令默认设置离线模式，避免正常依赖解析隐式访问 registry；依赖未缓存时返回明确可重试失败。这不构成对恶意 build/test 代码的网络封锁。应用自身除 provider 外不主动访问其他出站目标。

## 9. Provider 兼容子集

第一版只支持一个 OpenAI-compatible Chat Completions 风格 endpoint：

- 配置：`base_url`、`model`、`api_key`、连接/请求超时；
- `POST /v1/chat/completions`；
- system/user/assistant/tool messages；
- tools 数组、`tool_choice=auto`；
- assistant 的单个 tool call；
- tool result 通过对应 `tool_call_id` 返回；
- 非流式 JSON 响应；
- 不使用 vendor 私有 reasoning 字段、response cache、files、web search 或并行工具调用。

HTTP client 禁止自动 redirect，Authorization 永远不跨 origin 转发；默认禁用内容自动解压，若实现解压则对解压后字节另设硬上限。每轮 request、response body 和整个任务累计 provider 字节分别受限。response body 在聚合和 JSON 解析前按流计数，不能依赖 `Content-Length`，超限立即取消读取并返回稳定错误；30x、无长度 chunk 洪泛、超大 JSON 与压缩炸弹都是离线 contract 测试。

生产 provider transport 固定使用 rustls，不依赖系统 native TLS。可复用 client 只持有不可变配置与连接池；每个 agent run 通过 `start_task` 创建独立 provider session，累计字节预算与响应 metadata 不跨 task 共享。默认 connect/request timeout 分别为 10/120 秒，request/response/task provider 字节上限分别为 1 MiB/1 MiB/8 MiB；成功响应若回显配置 API key，也必须在 provider 边界以稳定错误拒绝。

如果响应包含多个 tool calls，Project 2 返回 `PROVIDER_UNSUPPORTED_MULTIPLE_TOOL_CALLS`，不猜测顺序。Project 3 再定义确定性多调用处理。

配置来自应用私有 data directory 下的 `provider.json`，严格 schema 为 `{ "base_url": "https://...", "model": "...", "api_key": "..." }`，其中 API key 必须是 8–4096 字节的可打印 ASCII；拒绝未知字段、非 HTTPS 远端 URL、带 userinfo/query/fragment 的 base URL 和非私有文件权限；测试可显式允许 loopback HTTP。配置只在成功获得 primary instance lock 后读取，因此 secondary launch 不依赖配置。缺少或无效配置时 primary 启动失败并给出稳定配置错误，不得悄悄回落到 fake runner。测试通过依赖注入使用 fake/mock runner。

## 10. 单角色 Agent 循环

输入由固定 system policy、任务 prompt、仓库/Cargo 元数据和工具 schema 组成。循环每轮只接受两类结果：

1. 一个结构合法的 tool call：执行后加入截断过的 tool result，并继续；
2. 一个最终文本：runner 先采集最终 Git diff，并验证最近一次真实测试绑定当前 workspace revision，再决定成功或失败。

任务 prompt、仅含 package/integration-test selector 的仓库上下文、工具结果和最终文本都在 provider 边界执行相同的 secret redaction；若 tool-call id 或参数经过 redaction 会发生变化，则以 `PROVIDER_SECRET_DETECTED` 在执行前 fail closed。普通工具失败作为带 `tool_status=failed` 与明确 `truncated` 标记的有界结果继续循环；provider/runtime 原始消息、响应体和 chain-of-thought 不进入事件或 outcome。

runner 在内存中维护单调 `workspace_revision`：每次成功 `replace_file` 后递增，并立即发布 queued test snapshot 使旧 passed 视图失效。由于 Cargo/build/test 或其他受信宿主进程也可能改写代码，测试开始前、测试结束后和最终成功判断前都通过受控 Git plumbing 计算 workspace fingerprint。

fingerprint 集合与最终可交付 diff 集合一致：包含所有 tracked 文件的 index 身份/状态/内容，以及所有非 ignored untracked 文件的相对路径、类型和内容；仓库自身 `.gitignore`/`.gitattributes` 生效，但固定清空 system/global excludes 与 attributes，避免宿主配置隐藏交付物。排除 Git ignored 文件和目录（包括正常 `target/` 构建制品），拒绝 symlink/reparse、gitlink/submodule、unmerged index、assume-unchanged 与 skip-worktree/sparse 状态。输入按原始路径字节的确定性顺序、handle-relative no-follow 方式流式哈希，并在前后重复校验 Git 列表、状态、文件 metadata 与 namespace identity；文件数、单文件字节或累计字节安全上限一旦触发就 fail closed，不能用截断 fingerprint 证明测试有效。只有开始与结束 fingerprint 相同且最终 fingerprint 仍相同的 passed 测试才能绑定当前 revision。任何不一致都会递增/刷新 revision、发布 queued 失效状态并要求重测。正常成功要求至少一次必需测试在当前 revision 和 fingerprint 上 passed；Project 3 再把该证据提升为持久 generation/diff digest。

每次成功替换后立即采集稳定 fingerprint/diff/fingerprint 快照并发布 neutral diff；terminal success/failure/cancel 再强制采集一次。terminal diff 任一文件被截断时仍保留快照，但以 `TERMINAL_DIFF_TRUNCATED` 拒绝 Completed。模型轮数/工具调用耗尽使用 retryable `AGENT_STEP_LIMIT_REACHED`，provider/context/tool-result 字节耗尽使用 retryable `AGENT_CONTEXT_LIMIT_REACHED`。

停止条件：

- 最大模型轮数；
- 最大累计 provider 输入/输出字节；
- 最大累计工具调用数；
- task cancellation；
- provider/runtime 不可恢复错误；
- 模型明确结束。

达到预算但没有正常结束时返回 retryable failure，而不是 Completed。模型最终文本不等于审查结论。

## 11. 事件投影

沿用 Project 1 的四类 runner 事件：

- `PlanUpdated`：初始为“检查仓库、修改代码、运行验证”三步；工具进度驱动 pending/running/completed；
- `ActivityAppended`：记录阶段、工具名称、相对路径、命令摘要和错误码，不记录 secret 或完整模型思维；
- `DiffUpdated`：每次成功文件替换后 debounce 采集；正常 success/failure/cancel terminal outcome 前强制采集，强制应用关闭导致的 Interrupted 只保证保留最近一次已持久化快照；domain、SQLite projection、REST/OpenAPI 与前端类型显式贯通 `truncated: bool`，patch 始终保留真实前缀而不追加伪造 sentinel；
- `TestUpdated`：cargo 命令开始为 running，结束映射 passed/failed/cancelled；修改文件后立即用 queued 覆盖旧 passed；仅使用当前 revision 的真实结果。

Project 2 不新增 chain-of-thought、provider 原始消息或任意 JSON 事件。需要持久化的 attempt/worktree 元数据通过新的版本化 store schema 保存，不塞入 activity 文本充当权威状态。

## 12. 持久化扩展

新增 `task_attempt_artifacts` 表，一条 task attempt 一行：

- `task_id` 主键/外键；
- `repository_id`；
- `attempt`；
- `base_commit`；
- `branch_name`；
- `worktree_path`；
- `state`：`reserved|ready|inconsistent`；
- `failure_code` 可空；
- `created_at`；
- `updated_at`；
- 唯一约束：branch name、worktree path、`(repository_id, task_id, attempt)`。

创建流程需要可重入。`CodingAgentRunner` 依赖 `AttemptArtifactStore` port，其 app adapter 只通过 `StoreWriter` 串行执行 reserve/ready/inconsistent mutation，runner 不直接写 SQLite：

1. 原子验证 task/repository/attempt 并 reserve 确定性的 base/branch/path；
2. 执行 Git side effect；
3. 重新验证 worktree、branch、base commit 和 git-dir 身份；
4. 原子标记 ready，之后制品一律保留；
5. 任一身份冲突标记 inconsistent 并失败，绝不覆盖或删除未知对象。

同一运行中的 runner 对 `reserved` 且尚无 side effect 的记录可安全重入并继续创建。应用重启后原 task 已按 Project 1 规则变为 Interrupted：此时完全不存在的 side effect 标记 `inconsistent`（审计含义为 abandoned），完整且身份匹配的 side effect 可标记 ready；部分存在或身份不符标记 inconsistent 并保留现场。崩溃点测试覆盖 reserve 前、reserve 后、Git 创建中、Git 创建后/ready 前和 ready 后。数据库只保存路径和 Git 身份，不保存 provider secret。

实现使用 schema migration v2：`task_attempt_artifacts` 通过复合外键绑定 `tasks(id, repository_id, attempt)`，并在数据库层唯一约束 branch/path。所有 reserve/ready/inconsistent 变更只经 `StoreWriter`；同值重放幂等，终态不可反向或互换。重启协调器以数据库行重建纯值 reservation，但仓库、git-dir、work-tree 与 artifact root authority 始终来自当前已验证的 `WorktreeProvisioner` capability；真实 Git/disk 集成测试覆盖 absent、ready、partial 和 mismatched 现场且不做清理。

## 13. 稳定失败码

至少定义：

- `GIT_HEAD_UNBORN`
- `WORKTREE_CREATE_FAILED`
- `WORKTREE_STATE_INCONSISTENT`
- `WORKTREE_PATH_ESCAPE`
- `WORKTREE_METADATA_PROTECTED`
- `FILE_NOT_UTF8`
- `FILE_TOO_LARGE`
- `FILE_CHANGED_SINCE_READ`
- `ATOMIC_REPLACE_FAILED`
- `COMMAND_NOT_ALLOWED`
- `COMMAND_TIMED_OUT`
- `COMMAND_OUTPUT_LIMIT`
- `PROCESS_TREE_CLEANUP_FAILED`
- `CARGO_METADATA_FAILED`
- `CARGO_DEPENDENCY_UNAVAILABLE_OFFLINE`
- `PROVIDER_CONFIG_INVALID`
- `PROVIDER_UNAUTHORIZED`
- `PROVIDER_RATE_LIMITED`
- `PROVIDER_RESPONSE_INVALID`
- `PROVIDER_UNSUPPORTED_MULTIPLE_TOOL_CALLS`
- `AGENT_STEP_LIMIT_REACHED`
- `AGENT_CONTEXT_LIMIT_REACHED`

失败对象继续使用既有 `TaskFailure { code, message, retryable }`。内部错误链进入脱敏后的本地日志，用户消息不得包含 API key、宿主绝对敏感路径或完整命令输出。

## 14. 测试策略

实现严格按 TDD 分层：

1. runtime 单元测试：路径规范化、`.git` 保护、symlink/junction 静态与竞态逃逸、digest 冲突、原子替换、输出截断、环境白名单；
2. process 集成测试：成功、非零退出、timeout、cancellation、孙进程清理，覆盖 Windows 和 Unix；
3. Git 集成测试：dirty 原仓库不进入 worktree、分支/目录唯一、retry 隔离、创建失败恢复、hook 不执行、外部 filter/config 拒绝、external diff/textconv 不执行；
4. provider contract 测试：本地 mock HTTP server 验证 request schema、tool_call_id、鉴权脱敏、读取前 body 上限、禁止 redirect/自动解压和错误映射；
5. core 状态机测试：工具循环、预算、非法工具、取消和 terminal 采集；
6. app 集成测试：真实 `CodingAgentRunner` 通过既有 sink 产生可重放 plan/activity/diff/tests；
7. 离线端到端：临时 Rust Git repo + mock provider，模型脚本读取文件、替换内容、执行 `cargo test`，最终 task Completed 且原工作目录不变；另测“test passed 后再次 replace”、“测试程序修改 tracked 文件后退出 0”和外部进程在测试后修改文件都会使旧结果失效；
8. 失败端到端：测试失败、provider 断连、超时、取消、输出洪泛和重启中断。

默认 CI 不访问真实 provider。真实 provider smoke 只能显式启用且不得打印 secret。

## 15. UI 与配置

Project 2 只做运行真实任务所需的最小 UI 改动：

- 明确显示任务在隔离 worktree 执行；
- 明确警告 Cargo 会以当前用户权限执行仓库原有及模型生成的代码，Project 2 未提供恶意代码 OS 沙箱；
- Completed 文案不得暗示 reviewed/mergeable；
- activity 显示工具和命令摘要；diff/test 继续使用既有面板；
- bootstrap 的实际并发值为 1。

第一版 provider 配置使用第 9 节锁定的私有 `provider.json`，Web UI 不回显 API key。完整 provider 设置 UI 可后续增加，但不能降低 secret 边界。production runner factory 只在 primary lock 获取并完成私有路径准备后构造；factory 同时返回 runner 与并发值，真实 runner 固定为 1，测试 fake runner 可显式使用 4，bootstrap 始终返回实际值。

## 16. 验收门

Project 2 只有在以下全部成立时完成：

1. 生产 composition root 使用 `CodingAgentRunner`，并发固定为 1，缺配置不会回退 fake；
2. 每个 attempt 拥有独立 branch/worktree，dirty 原目录内容未被读取、修改或带入；
3. mock provider 能驱动读取、修改、测试的完整闭环；
4. 第一方文件工具和命令 cwd 不能逃离 worktree，`.git` 元数据不可被模型读取或修改，敏感环境不进入子进程；Cargo 所执行代码遵循第 2.3 节信任边界；
5. timeout/cancel 会清理子进程树，输出洪泛不会死锁或无限增长；
6. 单文件替换满足 digest 校验和平台安全替换语义；
7. plan/activity/diff/tests 经 SQLite 与 SSE 重放后结果一致，修改发生在 passed 测试后会先持久化测试失效事件；fingerprint 覆盖 tracked 与非 ignored untracked，忽略构建制品且任何上限都 fail closed；强制应用关闭的 Interrupted 允许保留最近一次已持久化 diff；
8. Completed 仍为 Unreviewed 语义，系统没有 review/merge 暗示；
9. 所有默认测试离线可重复，并在 Windows、macOS、Linux CI 通过；
10. 规格、分步实现计划、独立代码审查、新鲜验证结果和验收演示齐备。

## 17. 实施顺序约束

书面规格复核通过后再编写逐步实现计划。计划应优先打通不可伪造的安全底座：worktree → 路径/文件 runtime → 进程树 → provider contract → agent loop → app/store/UI composition → 离线 E2E。每一步先写失败测试，再写最小实现，不允许以端到端“大爆炸”提交替代分层门禁。
