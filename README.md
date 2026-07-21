# ngy 编码代理

本仓库包含本地浏览器编码代理的 Project 2 实现。该应用程序是一个 Rust 进程，
负责运行 Axum、SQLite、任务编排、原生对话框、隔离的 Git 工作树运行时，以及
兼容 OpenAI 的提供方客户端；React UI 则通过随机的 `127.0.0.1` 端口提供服务。

## 项目 2 范围

项目 2 同一时间只运行一个真实的编码任务。每次尝试都会基于已注册仓库所提交的
`HEAD` 获得一个唯一分支和一个私有 Git 工作树。代理可以检查并安全替换该
工作树内的文件，运行有界的 Cargo 命令，并通过项目 1 的持久化事件流发布
计划、活动、差异和测试证据。用户原工作目录中已暂存、未暂存及未跟踪文件的
字节内容不会复制到工作树中，也不会用作模型上下文。

`Completed` 表示执行循环正常结束，并且已有一项通过的 Cargo 测试与最终工作区
指纹绑定。它**不**表示已经审查、可交付、可合并或已自动合并。项目 3 将增加
独立的审查质量循环。

安装程序、macOS 应用程序包、Linux 桌面条目、代码签名与公证、自动更新以及
完善的启动器打包属于项目 4，不是项目 2 的 CI 门禁条件。

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

在第一个终端中启动 Axum：

```bash
cargo run -p coding-agent-app
```

进程会在下文列出的运行时目录中发布 `instance.json`。读取其中的 `port`，设置
明确的代理目标，然后在第二个终端中启动 Vite。在 Windows PowerShell 中：

```powershell
$descriptor = Join-Path $env:LOCALAPPDATA 'ngy\coding-agent\data\run\instance.json'
$port = (Get-Content -LiteralPath $descriptor | ConvertFrom-Json).port
$env:CODING_AGENT_AXUM_TARGET = "http://127.0.0.1:$port"
npm --prefix web run dev
```

在 Linux 上，先选择描述符路径，再启动 Vite：

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

在 macOS 上：

```bash
descriptor="$HOME/Library/Application Support/com.ngy.coding-agent/run/instance.json"
port="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1], 'utf8')).port)" "$descriptor")"
export CODING_AGENT_AXUM_TARGET="http://127.0.0.1:$port"
npm --prefix web run dev
```

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

生产环境的基础 URL 必须使用 HTTPS，且不能包含用户信息、查询参数或片段。客户端
会向 `v1/chat/completions` 发送 Chat Completions 请求，拒绝重定向，使用 rustls
TLS 后端，并对连接、请求、响应和任务累计量施加上限。API 密钥必须由 8 到 4096
个可打印且非空格的 ASCII 字节组成。应用程序将所配置的密钥用于提供方授权和
边界脱敏；它不会把密钥从 `provider.json` 复制到 SQLite、模型消息、子进程环境、
活动事件或普通日志中。这不是通用的内容扫描机制：任务提示词和保留的仓库构件
属于持久化用户数据，可能含有用户提供的凭据，因此不要把机密粘贴到其中任何一处。

该文件必须是私有的普通文件，且不能是链接。在 Unix 上使用 `0600` 模式，例如
`chmod 600 provider.json`。在 Windows 上，确保只有当前用户可以访问；应用程序
会验证已打开的文件句柄，并拒绝重解析点或权限过宽的 ACL。数据目录也应保持私有。

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

每次尝试的构件都会保留以供检查。分支采用
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

请使用 Web UI 中的应用程序菜单，并选择 **Quit local application**。进程会关闭
变更门禁，妥善收尾进行中的工作，持久化被中断的任务，尝试执行最终的 SQLite
检查点，移除描述符，并释放单实例锁。如果出现降级警告或
`unclean-shutdown.json`，说明正常的持久化/检查点流程未能干净完成。关闭标签页
或整个浏览器**不会**停止任务或应用程序。

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

Git 工作树和基于能力的文件工具会将每次尝试与用户的原工作目录隔离，但项目 2
**不是**面向不受信任代码的操作系统沙箱。Cargo 可能以当前操作系统用户的权限
执行现有或生成的 `build.rs`、过程宏、依赖项、测试二进制文件及其他仓库代码。
这些代码可以尝试读写工作树之外的内容、访问网络或启动进程。只应对这样的仓库
运行任务：您愿意以当前用户身份执行该仓库现有代码及任务生成的变更；对于真正
不受信任的代码，请使用单独加固的虚拟机或容器。

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
| `PROVIDER_CONFIG_INVALID` | 创建符合严格模式定义且保持私有的 `provider.json`；检查 HTTPS、字段名、文件权限、模型和 API 密钥。生产环境绝不会回退到假运行器。 |
| `PROVIDER_UNAUTHORIZED` | 检查所配置的 API 密钥和提供方账户，但不要把密钥写入日志或问题报告。 |
| `PROVIDER_RATE_LIMITED` / `PROVIDER_UNAVAILABLE` / `PROVIDER_TRANSPORT_FAILED` | 提供方触发限流、发生故障、断开连接或超时。请在提供方恢复正常后重试。 |
| `PROVIDER_REQUEST_BYTE_LIMIT_REACHED` / `PROVIDER_TASK_BYTE_LIMIT_REACHED` | 与提供方的交互超过了单次请求或任务累计字节预算。请缩小任务范围后重试。 |
| `PROVIDER_RESPONSE_INVALID` / `PROVIDER_UNSUPPORTED_MULTIPLE_TOOL_CALLS` / `PROVIDER_REDIRECT_REJECTED` | 端点未遵守受支持的单工具调用 Chat Completions 契约。请检查提供方兼容性和基础 URL。 |
| `GIT_HEAD_UNBORN` | 创建任务之前，请先提交仓库的初始修订版。系统有意不使用工作目录中的未提交字节。 |
| `WORKTREE_CREATE_FAILED` / `WORKTREE_STATE_INCONSISTENT` | 检查保留的尝试构件。请手动解决分支/路径标识冲突；应用程序不会删除未知对象。 |
| `WORKTREE_PATH_ESCAPE` | 预留的尝试路径或 Cargo 工作区逃逸出了受信任的仓库/构件根目录。请检查已注册的仓库和保留的构件，而不要削弱边界。 |
| `FILE_NOT_TEXT` / `FILE_TOO_LARGE` / `FILE_CHANGED_SINCE_READ` / `ATOMIC_REPLACE_FAILED` | 无法安全读取或原子替换所请求的文件。请先检查保留的工作树状态，再重试。 |
| `COMMAND_NOT_ALLOWED` / `COMMAND_TIMED_OUT` / `PROCESS_TREE_CLEANUP_FAILED` | 某个命令或 `.git` 路径违反了固定工具契约、发生超时，或无法安全进入静止状态。有界输出会作为明确标注已截断的工具证据返回。请检查工作树并终止任何可疑的仓库进程，然后再重试。 |
| `CARGO_METADATA_FAILED` / `CARGO_DEPENDENCY_UNAVAILABLE_OFFLINE` | 修复 Cargo 工作区或明确预取其依赖项；任务中的 Cargo 命令不会从网络获取内容。 |
| `AGENT_STEP_LIMIT_REACHED` / `AGENT_CONTEXT_LIMIT_REACHED` | 有界代理循环在得到有效最终结果前耗尽了步骤或上下文预算。请缩小任务范围后重试。 |
| `CURRENT_TEST_REQUIRED` | 最终工作树指纹没有对应的已通过 Cargo 测试证据。请在最后一次源代码更改后运行相关测试。 |
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
| `TASK_NOT_FOUND` / `TASK_NOT_RETRYABLE` / `TASK_NOT_CANCELLABLE` | 请刷新任务状态；该任务不存在，或其持久化状态已不再允许执行该操作。 |
| `NETWORK_ERROR` / `INTERNAL_ERROR` | 请重新打开应用程序并重试一次；如果错误再次发生，请保留请求 ID 和日志。 |
| `SHUTDOWN_PERSISTENCE_FAILED` | 进程以非零状态退出，并为下次启动留下恢复状态；请保留数据目录并重新启动。 |
