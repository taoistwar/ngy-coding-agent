# ngy 编码代理

本仓库包含本地浏览器编码代理已完成并通过验收的 Project 4（P4-A + P4-B）实现。该应用程序是一个 Rust 进程，
负责运行 Axum、SQLite、任务编排、原生对话框、隔离的 Git 工作树运行时，以及
兼容 OpenAI 的提供方客户端；React UI 则通过随机的 `127.0.0.1` 端口提供服务。

## Project 4 范围

P4-A 在 Project 3 的质量闭环上增加受控并发和资源准入。默认最多同时运行两个任务，
同一仓库也最多运行两个；每个尝试仍基于已注册仓库所提交的 `HEAD` 获得唯一分支和
私有 Git 工作树。代理可以检查并安全替换该工作树内的文件，运行有界的 Cargo 命令，
并通过持久化事件流发布计划、角色活动、差异、测试和逐轮审查证据。用户原工作目录中
已暂存、未暂存及未跟踪文件的字节内容不会复制到工作树中，也不会用作模型上下文。

一个任务顺序运行 `Planner #1 -> Executor #1 -> Reviewer #1`。Planner 只运行一次；
Reviewer 要求修改时，只在 Executor 和 Reviewer 之间返工，最多两次返工、三轮审查。
所有角色共享同一个任务 worktree、取消域、provider task session 和预算账本，但每次
角色运行都使用新的 transcript。角色之间只传递已经验证且有界的计划、findings、
检查证据和 workspace checkpoint，不传递隐藏推理或原始 provider 响应。

角色权限是硬边界：Planner 只能只读分析，不能运行 Git、Cargo 或写文件；Executor
可以使用受限的检查、写入、Git 和 Cargo 工具；Reviewer 可以只读检查完整差异并运行
批准的 Git/Cargo 验证，但不能替换文件。Reviewer 的 `approved` 只有在最终 workspace
generation、digest、完整 diff coverage 和全部必需检查一致时才生效。

生命周期与交付质量分别显示。新质量闭环只有在最终批准时产生
`Completed + ReviewApproved`；第三轮有效的 `changes_requested` 产生
`Failed + ReviewRejected`。取消、阻塞、provider/runtime/store 错误或预算耗尽均保持
`Unreviewed`，不能伪装成审查拒绝。Project 2 或更早的历史 `Completed` 任务迁移后也
保持 `Completed + Unreviewed`，不会生成伪造的 review evidence。

`ReviewApproved` 是本应用自动 Reviewer 对有界证据的判断，**不**代表人工审查、已经
合并、可部署、已签名或生产安全。它只允许用户进入受控的本地交付流程；应用绝不会因
Reviewer approval、页面刷新、后台调度或启动恢复而自动 merge。保留的 Git 工作树和
分支仍是供用户独立检查的权威构件。

Project 4 的批准范围固定为 P4-A + P4-B。它不提供自动 merge/cleanup、远程 push、PR、
rebase、squash 或自动冲突解决。构件历史/保留期/长期配额生命周期属于未来 P4-C；
动态运行时设置、安装程序、代码签名、公证、自动更新和真实 provider 冒烟属于未来 P4-D。
P4-C/P4-D 不是 Project 4 的未完成部分。

最终代码候选 `8da9d760f281527cc6d6806f226ab6e09f6015e0` 已在
[GitHub Actions run 33305748048](https://github.com/taoistwar/ngy-coding-agent/actions/runs/33305748048)
完成 7 个必需作业并全部成功；逐作业证据和验收边界见
[P4-B 实施计划的最终验收记录](docs/superpowers/plans/2026-08-04-controlled-delivery-merge-cleanup.md#实施完成与最终验收记录)。

## 受控本地交付

只有当前 attempt 同时为 `Completed + ReviewApproved`，最终 review generation、工作区
fingerprint、检查证据和保留构件仍完全一致，而且没有存活或结果未知的任务进程时，
Delivery panel 才会允许预检。服务端会重新观察已登记仓库**当前 checkout 的 symbolic
本地分支**及其 HEAD；应用不会替用户 checkout、switch，也不能把目标改成任意分支。
目标处于 detached HEAD、工作树不干净、HEAD 已变化、存在 ignored path collision、
不安全 Git 配置或其他 Git 操作时，交付会被拒绝。

预检只计算候选 tree、目标身份和冲突结果，不修改目标分支、目标 index 或目标工作树。
预检显示的 generation、fingerprint、source、target branch 和 target HEAD 必须保持新鲜；
用户随后还要对同一个 Ready operation/version 进行第二次明确确认。只有这次确认被持久
接受后，应用才会把已批准工作区固化为具有固定元数据的本地 source commit，并对刚才
认证的目标执行固定 `--no-ff` merge。成功的 merge commit 必须有目标和 source 两个
精确 parent；不提供 fast-forward、squash、rebase 或自定义 strategy。

冲突预检和实际 merge 冲突都不会提交目标修改。UI 只显示有界的相对冲突路径摘要，
不提供自动编辑或自动解决；请在外部修复目标状态后重新预检。网络断开、页面刷新或
HTTP 回复丢失不会生成新的请求身份，UI 会从 SQLite 中的 operation projection 恢复并
继续 polling。若 Git 子进程结果或现场身份无法证明，操作进入恢复/隔离状态，不会猜测
成功，也不会把未知结果当作普通重试。

merge 成功后，source worktree 和 source branch **默认永久保留**。移除 worktree 与
删除 source branch 是两个独立按钮、两个确认对话框和两个幂等 receipt：移除 worktree
不会顺带删分支；只有 worktree 已移除、source 已证明合入目标且目标 branch/HEAD 仍与
确认值一致时，才允许原子删除 source branch。两步都不使用 force，不执行
`reset --hard`、`clean`、stash 或 `branch -D`。移除前的“clean”也包含没有 ignored 或
untracked 文件；例如保留的 Cargo `target/` 构建输出需要由用户先在应用外清理。应用会把
首次请求遇到的这类现场作为可恢复的前置条件拒绝，不创建 cleanup receipt，也不升级成仓库
隔离；若请求已经持久化后现场才变脏，应用仍不会删除，并会按所处阶段记录失败或进入需要
人工检查的恢复/隔离状态。

这条路径只做本机 Git 交付，不会 fetch、pull、push、访问 remote、创建 PR 或联系代码
托管服务。应用内的 repository coordination 只约束本进程自己的任务和交付操作；它
**不是操作系统级或全局 Git 锁**。外部程序仍可改变仓库，因此每个副作用前后都会重新认证，
发现漂移时会停止并要求重新预检或人工检查。

## 前置条件

- Rust `1.97.0`，并安装 `rustfmt` 和 `clippy`（版本由
  `rust-toolchain.toml` 固定）。
- Node.js 24 或更高版本，以及 npm。
- Git 2.45 或更高版本；为发现仓库，还须确保可通过 `PATH` 使用 Cargo。
- 一个符合下文说明的私有 `provider.json`。生产环境启动时，如果该文件缺失或
  无效，不会回退到假测试运行器。
- 主机操作系统所需的原生构建依赖；使用浏览器或仓库选择器时还需要图形桌面
  环境。

运行以下命令安装一次 JavaScript 依赖：

```bash
npm --prefix web ci
```

## 使用 Vite 和 Axum 开发

不含 `embedded-web` 的调试构建会有意通过唯一的公共 Vite Origin
`http://127.0.0.1:5173` 提供 UI。Axum 仍会绑定随机的回环端口，并继续执行
Host 和 Origin 精确匹配、会话及 CSRF 检查。开发环境不存在身份验证绕过机制。

### 在第一个终端中启动 Axum：

```bash
cargo run -p coding-agent-app
```

### 第二个终端中启动 Vite

只有第一个终端中的主进程成功启动并保持运行时，它才会在下文列出的运行时目录中
发布 `instance.json`；正常退出时会移除该文件。请等到 `cargo run` 完成编译并开始
运行应用程序，再读取其中的 `port`、设置明确的代理目标，然后在第二个终端中启动
Vite。

#### 在 Windows PowerShell 上：

```powershell
$descriptor = Join-Path $env:LOCALAPPDATA 'ngy\coding-agent\data\run\instance.json'
if (-not (Test-Path -LiteralPath $descriptor -PathType Leaf)) {
    throw 'instance.json 尚未生成；请确认第一个终端中的 coding-agent-app 仍在运行且没有启动错误。'
}
$port = (Get-Content -LiteralPath $descriptor | ConvertFrom-Json).port
$env:CODING_AGENT_AXUM_TARGET = "http://127.0.0.1:$port"
npm --prefix web run dev
```

仅存在 `instance.lock` 并不表示主进程正在运行；该锁文件会永久保留。如果
`instance.json` 不存在，请回到第一个终端重新运行 `cargo run -p coding-agent-app`，
并先处理其中的启动错误，不要继续启动 Vite。

#### 在 Linux 上：

先选择描述符路径，再启动 Vite：

```bash
if [ -n "${XDG_RUNTIME_DIR:-}" ]; then
  descriptor="$XDG_RUNTIME_DIR/coding-agent/instance.json"
else
  descriptor="${XDG_DATA_HOME:-$HOME/.local/share}/coding-agent/run/instance.json"
fi
port="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1], 'utf8')).port)" "$descriptor")"
export CODING_AGENT_AXUM_TARGET="http://127.0.0.1:$port"
npm --prefix web run dev
```

#### 在 macOS 上：

```bash
descriptor="$HOME/Library/Application Support/com.ngy.coding-agent/run/instance.json"
port="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1], 'utf8')).port)" "$descriptor")"
export CODING_AGENT_AXUM_TARGET="http://127.0.0.1:$port"
npm --prefix web run dev
```

#### 辅助进程认证

Vite 就绪后，再运行一次 `cargo run -p coding-agent-app`。这个短暂运行的辅助
进程会向主进程请求新的单次使用 URL，并在已配置的 Vite Origin 打开 UI。
请让第一个 Axum 进程保持运行。

## 生产构建与直接启动

编译嵌入式 Rust 构件之前，先构建 React 资源：

```bash
npm --prefix web ci
cargo run --locked -p coding-agent-api --bin export_openapi -- web/openapi.json
npm --prefix web run api:check
git diff --exit-code -- web/openapi.json web/src/api/generated/schema.d.ts
npm --prefix web run typecheck
npm --prefix web run test:run
npm --prefix web run build
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo build --release --locked -p coding-agent-app --features embedded-web
```

生成的文件在 Linux/macOS 上是 `target/release/coding-agent-app`，在 Windows
上是 `target\release\coding-agent-app.exe`。它包含 React 资源，运行时不需要
Node 或 `web/dist`，不接受常规 CLI 参数，并且可以复制到任意目录后启动。

在 Linux/macOS 上用 `./target/release/coding-agent-app` 直接启动；在 Windows
PowerShell 中则使用 `.\target\release\coding-agent-app.exe`。

启动第二个副本不会创建另一个数据库写入器。它会验证现有主进程，请求新的
单次使用 URL，打开浏览器，然后退出。辅助进程不会读取或验证 `provider.json`。

## 受控并发与 runtime.json

应用从数据目录中的 `runtime.json` 读取启动期运行参数。文件缺失时使用已记录的默认值：
全局并发 2、同仓库并发 2、队列上限 32，以及两个 2 GiB 的数据卷保留值。若需要覆盖，
必须提供下面这个完整且严格的对象：

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

`max_concurrent_tasks` 和 `max_concurrent_tasks_per_repository` 的范围都是 1–4，且后者
不能大于前者；`max_queued_tasks` 的范围是 1–256。两个 storage 值必须是非零 `u64`，
并且控制保留值加“每任务保留值 × 全局并发”的计算不能溢出。对象缺字段、包含未知或
重复字段、版本不匹配、值越界或文件不私有时，启动会以 `RUNTIME_CONFIG_INVALID`
fail closed；只有文件确实不存在时才会回退到默认值。该配置只在主进程启动时读取，
运行中修改不会动态生效，Web UI 也不提供修改入口。

`runtime.json` 与 `provider.json` 采用相同的私有普通文件要求：不能是链接或重解析点；
Unix 上使用 `0600`，Windows 上只允许当前用户访问。各平台的数据目录见下文。每任务
Cargo 并行度由启动时可用并行度自动计算为
`max(1, min(8, available_parallelism / max_concurrent_tasks))`；无法读取可用并行度时为 1。

Scheduler 按任务创建时间和任务 ID 确定性选择任务，同一仓库不超车；某个仓库暂时
受阻时，其他仓库可以继续。服务端只投影以下五种队列原因，UI 不显示队列位置或 ETA：

- `service_paused` — Waiting for the service
- `storage_pressure` — Waiting for storage
- `global_capacity` — Waiting for global capacity
- `repository_capacity` — Waiting for repository capacity
- `repository_control_busy` — Waiting for repository coordination

全局队列达到上限时，全新的创建或 retry 返回 `TASK_QUEUE_FULL`；幂等重放仍按原
`client_request_id` 解析，结果未知或 queue-full 的原命令不会被 UI 静默改成新请求。
升级前已经存在且超过新上限的有限 legacy 队列会如实保留并自然排空，而不是被截断。

空间监控只覆盖应用数据卷、各已注册仓库的 Git 卷和 runtime 卷。`pressure` 或
`unavailable` 会阻止新的准入，但通常不会停止已经运行的任务；`critical` 会为受影响
任务提交持久的 `disk_pressure_critical` 停止意图。数据卷的下一候选阈值使用
`data_control_reserve_bytes + data_task_reservation_bytes × min(全局上限, active + 1)`；
Git/runtime 卷的准入阈值是 256 MiB，紧急阈值是 64 MiB。恢复准入需要两次满足恢复
余量且至少相隔 5 秒的成功样本。共享物理卷只采样一次并应用最严格谓词。

这些值是应用管理范围内的准入和安全保留，不是主机磁盘硬配额；公开状态也不会暴露
原始可用字节、路径或卷身份。P4-A 同样不限制主机 CPU、内存、进程数或网络使用。

## 提供方配置

按照下表所示路径，在数据目录中创建 `provider.json`。该文件采用单一且严格的
模式定义；未知字段会被拒绝：

```json
{
  "base_url": "https://provider.example/",
  "model": "provider-model-name",
  "api_key": "replace-with-the-provider-key"
}
```

对于默认开启思考模式的 DeepSeek V4，请显式加入 `thinking`。如果像海马云这样的
兼容中转站不能正确处理命名函数 `tool_choice`，还应显式启用
`tool_choice_compatibility`：

```json
{
  "base_url": "https://provider.example/",
  "model": "deepseek-v4-flash",
  "api_key": "replace-with-the-provider-key",
  "thinking": "enabled",
  "tool_choice_compatibility": "required_as_auto"
}
```

### 开发私网 HTTP（显式且不安全）

默认配置和生产环境都要求 HTTPS。仅当开发测试环境中的提供方确实没有 TLS 时，才可
同时使用私网 HTTP URL 并显式设置 `"allow_insecure_http": true`：

```json
{
  "base_url": "http://172.16.1.20:19001/",
  "model": "deepseek-v4-flash",
  "api_key": "replace-with-the-provider-key",
  "thinking": "enabled",
  "tool_choice_compatibility": "required_as_auto",
  "allow_insecure_http": true
}
```

`allow_insecure_http` 是可选布尔值，省略或设为 `false` 时不会放宽 HTTPS 要求。即使
显式设为 `true`，HTTP 主机也必须是 IP 字面量，并且只能属于 IPv4 RFC 1918 私网、
IPv6 ULA 或回环地址。DNS 主机名、公网地址以及 IPv4/IPv6 链路本地地址仍会被拒绝；
该开关也不会放宽对 userinfo、查询参数、片段或重定向的限制。

此模式没有 TLS。Bearer API key、任务提示词、发送给模型的仓库内容、工具结果和模型
响应都会在网络上以明文传输，也可能被路径上的设备读取或篡改。它只能用于隔离且受信任
的开发网络，绝不能用于生产环境；条件允许时，即使在私网中也应优先部署 HTTPS。修改
配置后必须完全退出并重新启动应用程序。

`thinking` 是可选配置，允许值为 `enabled` 和 `disabled`，客户端分别编码为
`{"thinking":{"type":"enabled"}}` 和 `{"thinking":{"type":"disabled"}}`。
启用后，模型在工具调用响应中返回的非空 `reasoning_content` 会作为不透明协议状态
留在当前任务内，并随对应 assistant 工具调用消息回传给提供方，以支持多轮思考；它
不会显示为最终回答或写入持久化任务数据。该内容仍受响应字节预算和 API 密钥边界检查。
关闭或未配置思考时，只接受省略、`null` 或空字符串形式的 `reasoning_content`，非空
内容会被拒绝。工具调用消息同时携带的普通 assistant 文本也会经过相同检查并在下一轮
请求中原样回传；它不会被误当成思维链。若中转站在一个 assistant 消息中返回多个工具调用，应用会先原子校验整个
批次，再严格按数组顺序逐个执行，最后以一条 assistant 消息和同序的全部工具结果
继续对话；任何后置调用无效、含秘密或超出剩余预算时，整个批次都不会开始执行。
不要为不支持 `thinking` 参数的普通 OpenAI-compatible 模型添加此项。

`tool_choice_compatibility` 也是可选配置。省略它或设置为 `strict` 时，客户端保持
标准命名函数编码。`required_as_required` 只改变强制验证请求在线路上的表示：客户端
发送 `tool_choice="required"`；`required_as_auto` 则用于 thinking 模式不接受强制选择的
DeepSeek V4 兼容线路，发送 `tool_choice="auto"`。两种兼容模式都会把可见工具缩减为唯一的
`cargo_test`，核心逻辑约束不变：响应仍必须恰好包含一个 `cargo_test`，最终文本、其他工具
或多个调用都会被拒绝。兼容模式不会在失败后自动补发第二次 HTTP 请求，也不会按域名或
模型名猜测中转站能力。
DeepSeek [官方 Chat Completions 契约](https://api-docs.deepseek.com/zh-cn/api/create-chat-completion/)
支持通用 `"required"`，但这并不能证明任意中转站都会完整透传；仍须通过该中转站
单独实测。不要仅因使用 DeepSeek 就启用它；只有线路
不能遵守命名工具选择、并且确认支持通用强制选择时才需要此项。修改配置
后必须完全退出并重新启动应用程序，再从失败的尝试明确重试。

### 多角色预算、收敛与工具选择

生产默认使用一个 Task 级共享硬预算；进入新角色不会重置它：

| 范围 | 模型响应 | model-visible calls |
| --- | ---: | ---: |
| 整个 Task | 60 | 96 |
| Planner | 8 | 12 |
| 每轮 Executor | 20 | 32 |
| 每轮 Reviewer | 10 | 16 |

角色上限之和可以高于 Task 上限，但共享账本总是先到先停。每次 provider HTTP request
和 response 各最多 1 MiB，整个 Task 的 encoded provider 流量最多 8 MiB；单项送回模型
的 tool result 最多 256 KiB，Task 级累计保留最多 768 KiB。Planner 的结果租约最多
128 KiB，每轮 Executor/Reviewer 各最多 256 KiB。无效但已经收到的 provider response
仍计费，已通过脱敏并保留的同一 tool result 在共享结果账本中只计一次。

进入 Executor 前，核心会同时为当前必需检查、交审控制动作以及紧随其后的 Reviewer
完整 manifest/chunk/terminal 路径做原子预留；Reviewer coverage 预留不会被普通探索
结果消费。如果 Reviewer 要求返工，但剩余额度不足以同时启动下一轮 Executor 和
Reviewer，任务以阶段化预算错误结束为 `Failed + Unreviewed`，不会把该轮升级成
`ReviewRejected`，也不会偷偷创建新的 provider session。

Planner、Executor 和 Reviewer 都必须以各自的类型化 control action 正常结束；普通
final text 不能冒充 `submit_plan`、`submit_execution` 或 `submit_review`。运行时工具
调用批次会先整体验证权限、预算、路径、调用 ID 和参数，再按 provider 顺序执行；批次中
任一后置调用无效时，整个批次保持零副作用。`tool_choice_compatibility` 只改变受限请求
在线路上的编码，不放宽本地的恰好一次控制动作、角色权限或响应 schema 校验。违反本轮
约束会以 `PROVIDER_RESPONSE_TOOL_CHOICE_VIOLATED` 失败，错误中不会包含响应正文。

除上述显式的开发私网 HTTP 例外外，基础 URL 必须使用 HTTPS；生产环境始终必须使用
HTTPS。基础 URL 不能包含用户信息、查询参数或片段。该值应
指向版本段之前的服务根或兼容前缀，**不要**在末尾包含 `/v1`；客户端会自行追加
`v1/chat/completions`。例如，提供方文档给出的地址若为 `https://provider.example/v1`，
这里应填写 `https://provider.example/`。客户端拒绝重定向，使用 rustls TLS 后端，
并对连接、请求、响应和任务累计量施加上限。API 密钥必须由 8 到 4096
个可打印且非空格的 ASCII 字节组成。应用程序将所配置的密钥用于提供方授权和
边界脱敏；它不会把密钥从 `provider.json` 复制到 SQLite、模型消息、子进程环境、
活动事件或普通日志中。这不是通用的内容扫描机制：任务提示词和保留的仓库构件
属于持久化用户数据，可能含有用户提供的凭据，因此不要把机密粘贴到其中任何一处。

该文件必须是私有的普通文件，且不能是链接。在 Unix 上使用 `0600` 模式，例如
`chmod 600 provider.json`。在 Windows 上，确保只有当前用户可以访问；应用程序
会验证已打开的文件句柄，并拒绝重解析点或权限过宽的 ACL。数据目录也应保持私有。

### Windows 快速配置

在 Windows 上，生产配置文件的默认位置是
`%LOCALAPPDATA%\ngy\coding-agent\data\provider.json`。可以先在 PowerShell 中运行
以下命令，用记事本打开该文件（文件不存在时请创建它）：

```powershell
$data = Join-Path $env:LOCALAPPDATA 'ngy\coding-agent\data'
New-Item -ItemType Directory -Force -Path $data | Out-Null
$provider = Join-Path $data 'provider.json'
notepad.exe $provider
```

将上面的严格 JSON 配置填入文件并保存。不要把真实 API 密钥粘贴到终端、问题报告或
聊天中，也不要把该文件加入 Git。随后用以下命令移除继承权限，并确保只有当前
Windows 用户拥有访问权：

```powershell
$me = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
icacls.exe $provider /grant:r "${me}:(F)"
icacls.exe $provider /inheritance:r
```

完成后重新启动应用程序：

```powershell
cargo run -p coding-agent-app
```

如果原生对话框显示 `Error code: PROVIDER_CONFIG_INVALID`，请首先检查上述文件是否
存在。配置缺失、JSON 不符合严格模式、ACL 不够私有，或者 HTTP URL 没有同时配置
`"allow_insecure_http": true`、主机不是允许的私网/回环 IP 字面量，都会阻止运行器
启动，并统一归入该错误码；这不是 Cargo 或 Git 编译失败。旧版本可能只显示重复的
`the coding task runner could not be started`，排查方法相同。

## 数据与运行时文件

应用程序使用主机上每个用户各自的项目目录：

| 操作系统 | 数据目录 | 运行时目录 |
| --- | --- | --- |
| Windows | `%LOCALAPPDATA%\ngy\coding-agent\data` | `%LOCALAPPDATA%\ngy\coding-agent\data\run` |
| macOS | `~/Library/Application Support/com.ngy.coding-agent` | 数据目录加 `/run` |
| Linux | `${XDG_DATA_HOME:-~/.local/share}/coding-agent` | 如已设置，则为 `$XDG_RUNTIME_DIR/coding-agent`；否则为数据目录加 `/run` |

`coding-agent.sqlite3` 是持久化数据库。`instance.lock` 和 `instance.json` 用于
协调正在运行的进程；描述符包含私有的启动器能力凭证，不得共享。
`unclean-shutdown.json` 会记录未能持久化最终任务状态的关机；应用程序会在
下次启动时根据该文件执行恢复。

计划、活动、diff/test panel、逐轮 review evidence、delivery readiness 和 lifecycle
事件都保存在 SQLite 中。中间 `changes_requested` evidence 不会在重启时丢失或被改写；
启动恢复会先处理未完成的持久化结果，再开放 Scheduler 并发布新的浏览器描述符。冷启动
保留原有 `Queued` 任务等待重新准入；崩溃时的普通 `Running` 任务恢复为
`Interrupted + Unreviewed`。若 `Running` 任务已有持久停止意图，则恢复遵守该意图：
`user_cancelled` 最终为 `Cancelled`，`disk_pressure_critical` 最终为可重试的 `Failed`。
用户需要对普通中断明确 retry；retry 会建立新的 attempt，不会继承旧 attempt 的 review
approval。只有最终 diff/test 已获得持久确认后，最终 review、readiness 和 terminal
lifecycle 才会在同一事务中提交。

每次尝试的构件默认都会保留以供检查；只有成功 merge 后，用户才能按上一节所述分别
确认移除 worktree 和删除 source branch。分支采用
`codex/task-<task-id>-attempt-<attempt>` 格式，工作树存储在私有数据目录下的
`worktrees/<repository-id>/<task-id>/<attempt>`。SQLite 会记录不可变的仓库、
任务及尝试标识，以及基础提交、分支、工作树路径和 `reserved`、`ready` 或
`inconsistent` 生命周期状态。重试始终会获得新的分支和工作树；应用程序绝不会
覆盖或删除未知的冲突构件。

若要仅备份主数据库文件，请通过 Web UI 退出，并确认应用程序已干净关闭：进程和
`instance.json` 均已消失、界面未显示降级关机警告、`unclean-shutdown.json`
不存在，并且没有留下非空的 `coding-agent.sqlite3-wal`。只有满足这些条件后，
才能复制 `coding-agent.sqlite3`。如果关机发生降级或仍有恢复标记/WAL，请保留
完整的数据目录，重新启动应用程序，等待恢复状态变为 Connected，再干净退出，
然后执行仅备份主数据库文件的操作。应用程序运行期间不得复制或替换数据库；
如果无法实现干净关机，请使用理解 SQLite 机制的恢复工具。只能在应用程序停止
时执行还原，并将备份与创建它的应用程序版本一同保存。

## 退出应用程序与恢复浏览器访问

请使用 Web UI 中的应用程序菜单，并选择 **Quit local application**。进程会先关闭
变更门禁和新的 delivery 准入，妥善收尾已经持久接受的任务与本地 Git 阶段，等待全部
已启动进程树得到退出证明，完成持久停止意图或留下可恢复的 delivery pending，再尝试
执行最终的 SQLite 检查点、移除描述符并释放
单实例锁。如果出现降级警告或
`unclean-shutdown.json`，说明正常的持久化/检查点流程未能干净完成。关闭标签页
或整个浏览器**不会**停止任务或应用程序。

对 `Running` 任务的用户取消只有在 `user_cancelled` 意图持久化后才会被确认，最终显示
`Cancelled`。critical storage 触发的是不同的 `disk_pressure_critical` 意图，最终显示
可重试 `Failed`；同一任务最先被接受的停止分类不会被后来的另一分类覆盖。对仍在
`Queued` 的任务取消会直接成为 `Cancelled`，不创建停止意图。

如果自动打开浏览器失败，请从原生错误对话框中复制完整的单次使用 URL，并手动
打开。如果标签页丢失或该 URL 已过期，请再次启动可执行文件；辅助进程会向通过
验证的主进程请求新的单次使用 URL。发生普通的浏览器打开失败后，主进程仍会
继续运行。

## 安全边界

服务器只监听随机的 IPv4 回环端口。严格的 Host 检查、单次启动交换、进程范围的
会话 Cookie、严格的 Origin 检查及 CSRF 令牌可防范普通的跨站浏览器访问。交换
完成后，启动器和提供方配置机密不会出现在 URL 及普通日志中；应用程序也不会
将配置的提供方密钥复制到 SQLite。任务提示词和保留的构件仍属于持久化用户
数据，其中可能包含以任务内容或仓库内容形式提供的机密。

Git 工作树和基于能力的文件工具会将每次尝试与用户的原工作目录隔离，但 P4-A
**不是**面向不受信任代码的操作系统沙箱。Cargo 可能以当前操作系统用户的权限
执行现有或生成的 `build.rs`、过程宏、依赖项、测试二进制文件及其他仓库代码。
这些代码可以尝试读写工作树之外的内容、访问网络或启动进程。只应对这样的仓库
运行任务：您愿意以当前用户身份执行该仓库现有代码及任务生成的变更；对于真正
不受信任的代码，请使用单独加固的虚拟机或容器。Executor 和 Reviewer 运行的 Cargo
检查具有相同的当前用户权限；“Reviewer 只读”限制的是应用提供的文件修改能力，并不把
仓库自身的构建脚本或测试二进制文件变成安全代码。

最终测试证据与实际的最终工作区指纹绑定。差异收集也会在收集前后检查工作区，
但它不提供文件系统快照，也不针对恶意的同用户进程提供线性一致性保证；这种
进程可能会在该时间窗口中故意更改字节，然后将其还原。在这种超出边界的场景下，
保留的工作树仍是权威构件。

内置工具会拒绝路径逃逸、访问 `.git`、链接/重解析点、无界输出、继承的机密、
任意命令和可执行的 Git 配置。Cargo 默认离线运行；超时或取消时会终止整个子
进程树。这些控制措施可以减少意外的工具逃逸，但不会改变上述受信任代码边界。
已经以同一操作系统用户身份运行的恶意进程还可以检查该用户的内存、文件、
浏览器流量或本地端点。仓库路径和运行时描述符应视为私有用户数据。

## 故障排除代码

UI 显示的错误包含稳定的代码和请求 ID。将错误与本地日志对应时，请同时保留
这两项信息。

| 代码 | 含义与处理方法 |
| --- | --- |
| `APP_STARTING` | 启动恢复仍在进行。请稍候片刻，再次启动可执行文件。 |
| `APP_SHUTTING_DOWN` | 关机流程已关闭变更门禁。请等待进程退出后再重新启动。 |
| `STORE_BUSY` | SQLite 未能在前台时间预算内接受写入。请使用 UI 中的明确重试操作；创建操作的重试会保留其请求 UUID。 |
| `STORE_DEGRADED` | 持久化恢复期间，变更和新任务领取均会暂停。请等待 Connected 横幅出现；只有恢复始终无法收敛时才重新启动。 |
| `APP_RESTARTED` | 崩溃/重启恢复期间，一个未完成的任务被中断。需要时请明确重试。 |
| `APP_SHUTDOWN` | 应用程序正常退出时，一个未完成的任务被中断。 |
| `STORE_WRITE_FAILED` | 某个任务存在结果不明确的后台写入，因此在恢复期间被持久化地标记为中断。 |
| `PROVIDER_CONFIG_INVALID` | 创建符合严格模式定义且保持私有的 `provider.json`；检查 HTTPS、字段名、文件权限、模型和 API 密钥。开发私网 HTTP 还必须显式设置 `"allow_insecure_http": true`，且主机只能是允许的私网/回环 IP 字面量；DNS、公网和链路本地 HTTP 会被拒绝。生产环境绝不会回退到假运行器。 |
| `PROVIDER_UNAUTHORIZED` | 检查所配置的 API 密钥和提供方账户，但不要把密钥写入日志或问题报告。 |
| `PROVIDER_REQUEST_REJECTED` | 提供方拒绝了请求。首先确认 `base_url` 末尾没有 `/v1`，因为客户端会自行追加版本路径；然后检查模型是否支持 Chat Completions、函数工具调用以及请求中的模型名。 |
| `PROVIDER_RATE_LIMITED` / `PROVIDER_UNAVAILABLE` / `PROVIDER_TRANSPORT_FAILED` | 提供方触发限流、发生故障、断开连接或超时。请在提供方恢复正常后重试。 |
| `PROVIDER_REQUEST_BYTE_LIMIT_REACHED` / `PROVIDER_TASK_BYTE_LIMIT_REACHED` | 与提供方的交互超过了单次请求或任务累计字节预算。请缩小任务范围后重试。 |
| `PROVIDER_RESPONSE_REASONING_REJECTED` | 未启用思考时，提供方仍返回了非空思维链。需要思考模型时设置 `"thinking":"enabled"`；需要关闭时确认模型和中转站支持 `thinking.type=disabled`。错误不会包含思维链正文。 |
| `PROVIDER_RESPONSE_TOOL_CHOICE_VIOLATED` | 提供方没有遵守本轮的自动、指定 `cargo_test` 或禁用工具约束。不支持命名函数选择但支持通用 `"required"` 时使用 `required_as_required`；DeepSeek V4 thinking 线路拒绝强制 `tool_choice` 时使用 `required_as_auto`。完全重启应用后再明确重试；响应仍须恰好返回一个 `cargo_test`。该错误不可自动重试，且不会包含响应正文。 |
| `PROVIDER_RESPONSE_FINISH_UNSUPPORTED` | 提供方返回了缺失、未知或与文本/工具调用不匹配的结束状态。请检查模型的 Chat Completions 工具调用兼容性。 |
| `PROVIDER_RESPONSE_SCHEMA_UNSUPPORTED` | 提供方返回的 JSON 可以解析，但字段或类型不属于受支持的严格响应模式。错误不会包含响应正文或未知字段名。 |
| `PROVIDER_RESPONSE_INVALID` / `PROVIDER_REDIRECT_REJECTED` | 响应不是有效 JSON、超出限制、工具调用批次无效或违反其他受支持的 Chat Completions 契约。若 DeepSeek V4 或其中转服务默认开启思考模式，请在 `provider.json` 加入 `"thinking": "disabled"` 并完全重启应用；其他模型请检查提供方兼容性和基础 URL。 |
| `PROVIDER_UNSUPPORTED_MULTIPLE_TOOL_CALLS` | 这是旧版本留下的历史失败码。当前版本已支持按响应顺序串行处理多个工具调用；完全重启新版本后，从该失败尝试重试即可。 |
| `GIT_HEAD_UNBORN` | 创建任务之前，请先提交仓库的初始修订版。系统有意不使用工作目录中的未提交字节。 |
| `WORKTREE_CREATE_FAILED` / `WORKTREE_STATE_INCONSISTENT` | 检查保留的尝试构件。请手动解决分支/路径标识冲突；应用程序不会删除未知对象。 |
| `WORKTREE_PATH_ESCAPE` | 预留的尝试路径或 Cargo 工作区逃逸出了受信任的仓库/构件根目录。请检查已注册的仓库和保留的构件，而不要削弱边界。 |
| `FILE_NOT_TEXT` / `FILE_TOO_LARGE` / `FILE_CHANGED_SINCE_READ` / `ATOMIC_REPLACE_FAILED` | 无法安全读取或原子替换所请求的文件。请先检查保留的工作树状态，再重试。 |
| `COMMAND_NOT_ALLOWED` / `COMMAND_TIMED_OUT` / `PROCESS_TREE_CLEANUP_FAILED` | 某个命令或 `.git` 路径违反了固定工具契约、发生超时，或无法安全进入静止状态。有界输出会作为明确标注已截断的工具证据返回。请检查工作树并终止任何可疑的仓库进程，然后再重试。 |
| `CARGO_METADATA_FAILED` / `CARGO_DEPENDENCY_UNAVAILABLE_OFFLINE` | 修复 Cargo 工作区或明确预取其依赖项；任务中的 Cargo 命令不会从网络获取内容。 |
| `*_STEP_LIMIT_REACHED` / `*_CONTEXT_LIMIT_REACHED` / `*_TASK_BUDGET_EXHAUSTED` | Planner、Executor 或 Reviewer 在得到有效类型化结果前耗尽角色额度、上下文或共享 Task 预算。Task 总上限为 60 次模型响应和 96 次 model-visible calls；Planner 为 8/12，每轮 Executor 为 20/32，每轮 Reviewer 为 10/16。若正常规模任务仍超限，请缩小范围后明确重试；系统不会借下一角色重置账本。 |
| `CURRENT_TEST_REQUIRED` | 最终工作树指纹没有对应当前修订的已通过 Cargo 测试证据。任何替换或指纹变化都会建立新修订并令旧证据失效；请在最后一次源代码更改后重新测试。 |
| `TERMINAL_DIFF_TRUNCATED` | 保留的最终差异超过安全上限，因此任务未被标记为 Completed。请检查工作树，并将变更拆分为较小的任务。 |
| `TERMINAL_FINALIZATION_TIMEOUT` | 最终测试/差异证据未能在有界的终态收尾时间窗口内稳定下来。请检查保留的工作树，并仅在所有仓库进程停止后重试。 |
| `TOOLCHAIN_DISCOVERY_FAILED` | 启动时无法固定所需的 Git/Rust 工具。请检查 Git 2.45+ 和当前 Rust 工具链，再重新启动。 |
| `RUNNER_PANICKED` | 运行器意外失败；该任务已被隔离，其他工作仍可继续。请保存请求 ID 和留存构件以供诊断。 |
| `REPOSITORY_PATH_NOT_FOUND` / `REPOSITORY_PATH_NOT_DIRECTORY` | 请选择一个现有目录。 |
| `CARGO_WORKSPACE_NOT_FOUND` / `CARGO_WORKSPACE_OUTSIDE_GIT_ROOT` | 请选择包含在其 Git 仓库内的 Cargo 工作区。 |
| `REPOSITORY_COMMAND_FAILED` | 请确认 Git 和 Cargo 已安装，并且可以检查该仓库。 |
| `PICKER_ALREADY_OPEN` / `PICKER_UNAVAILABLE` | 请完成当前选择操作，或直接输入仓库路径。 |
| `SECURITY_INVALID_SESSION` | 进程已重启或会话已过期。请再次启动可执行文件以获取新的单次使用 URL。 |
| `SECURITY_INVALID_HOST` / `SECURITY_INVALID_ORIGIN` / `SECURITY_INVALID_CSRF` | 请求并非来自当前应用程序页面。请重新打开可执行文件，而不要削弱检查。 |
| `SECURITY_INVALID_LAUNCHER_SECRET` / `SECURITY_INVALID_LAUNCH_TOKEN` / `SECURITY_DUPLICATE_HEADER` | 启动器能力凭证已过期、无效、被重放或含义不明确。请丢弃该 URL，并再次启动可执行文件。 |
| `INVALID_PROMPT` / `INVALID_REPOSITORY_PATH` | 请更正 UI 显示的任务文本或仓库路径。 |
| `IDEMPOTENCY_CONFLICT` | 同一请求 UUID 被用于不同内容。请从 UI 将其作为新操作重试。 |
| `TASK_NOT_MERGE_ELIGIBLE` / `DELIVERY_EVIDENCE_STALE` / `DELIVERY_SOURCE_CHANGED` / `DELIVERY_PREFLIGHT_STALE` | 当前任务、最终审查证据、source 工作区或预检版本已不再满足交付条件。刷新 Delivery panel；若任务仍 eligible，请运行一次新的预检，不要重放旧确认。 |
| `DELIVERY_OPERATION_IN_PROGRESS` | 同一 attempt 已有持久的交付操作。先查询并等待该 operation 收敛；不要生成新的请求 UUID 进行盲重试。 |
| `TARGET_BRANCH_DETACHED` / `TARGET_BRANCH_MISMATCH` / `TARGET_HEAD_CHANGED` / `TARGET_WORKTREE_DIRTY` | 已登记仓库的当前 checkout 与预检目标不一致，或预检目标/待移除的 retained source worktree 不再严格 clean。应用不会 checkout、reset、clean、stash 或删除 ignored/untracked 内容；请在外部清理正确的 worktree，确认目标仍是 clean symbolic branch，再刷新面板或重新预检。 |
| `TARGET_IGNORED_PATH_COLLISION` / `TARGET_GIT_OPERATION_IN_PROGRESS` / `UNSAFE_GIT_CONFIGURATION` / `UNSUPPORTED_GIT_ATTRIBUTES` | 目标存在 ignored collision、并发 Git 操作或不受支持的配置/attributes。先在仓库外部消除原因，再重新预检；不要放宽安全检查。 |
| `MERGE_CONFLICT` / `SOURCE_ALREADY_IN_TARGET` | 当前预检不能合法确认 merge，或 source 已经是目标祖先。冲突不会修改目标；检查有界相对路径摘要并在外部修复后重新预检。 |
| `ARTIFACT_CLEANUP_NOT_ALLOWED` / `ARTIFACT_PROCESS_STILL_ACTIVE` / `WORKTREE_IDENTITY_MISMATCH` / `SOURCE_BRANCH_NOT_MERGED` | 清理前置条件未满足。worktree 与 branch 默认保留；等待进程退出并刷新状态，只有成功 merge 后才能按顺序执行两个独立清理动作。 |
| `DELIVERY_RECONCILIATION_REQUIRED` / `DELIVERY_SOURCE_INCONSISTENT` / `REPOSITORY_CONTROL_POISONED` | 应用无法证明某个 Git 副作用或现场身份，已隔离相应仓库而不会猜测结果。保留完整数据目录和 Git 现场，重新启动让恢复先运行；仍未收敛时请人工检查，不要删除 receipt、refs 或数据库行。 |
| `REPOSITORY_CONTROL_BUSY` | 同一仓库正在执行受控任务或交付阶段。先查询当前状态，等已有操作完成后再重试；其他仓库仍可继续。 |
| `TASK_NOT_FOUND` / `TASK_NOT_RETRYABLE` / `TASK_NOT_CANCELLABLE` | 请刷新任务状态；该任务不存在，或其持久化状态已不再允许执行该操作。 |
| `NETWORK_ERROR` / `INTERNAL_ERROR` | 请重新打开应用程序并重试一次；如果错误再次发生，请保留请求 ID 和日志。 |
| `SHUTDOWN_PERSISTENCE_FAILED` | 进程以非零状态退出，并为下次启动留下恢复状态；请保留数据目录并重新启动。 |
