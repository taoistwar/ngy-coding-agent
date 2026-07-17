# ngy Coding Agent

This repository contains Project 2 of a local, browser-based Coding Agent. The
application is a Rust process that owns Axum, SQLite, task orchestration, native
dialogs, an isolated Git-worktree runtime, and an OpenAI-compatible provider
client, with a React UI served on a random `127.0.0.1` port.

## Project 2 scope

Project 2 runs one real coding task at a time. Every attempt receives a unique
branch and private Git worktree based on the registered repository's committed
`HEAD`. The agent can inspect and safely replace files inside that worktree,
run bounded Cargo commands, and publish plan, activity, diff, and test evidence
through the durable Project 1 event stream. The user's original staged,
unstaged, and untracked bytes are not copied into the worktree and are not used
as model context.

`Completed` means that the execution loop ended normally with a passing Cargo
test bound to the final workspace fingerprint. It does **not** mean reviewed,
deliverable, mergeable, or automatically merged. Project 3 adds the separate
review-quality loop.

Installers, a macOS application bundle, Linux desktop entries, code signing and
notarization, auto-update, and polished launcher packaging belong to Project 4.
They are not Project 2 CI gates.

## Prerequisites

- Rust `1.97.0` with `rustfmt` and `clippy` (pinned by
  `rust-toolchain.toml`).
- Node.js 24 or newer and npm.
- Git 2.45 or newer and Cargo available on `PATH` for repository discovery.
- A private `provider.json` as described below. Production startup does not
  fall back to the fake test runner when this file is absent or invalid.
- The native build prerequisites for the host OS and a graphical desktop when
  using the browser or repository picker.

Install JavaScript dependencies once with:

```bash
npm --prefix web ci
```

## Development with Vite and Axum

A debug build without `embedded-web` deliberately serves the UI through the
single public Vite origin `http://127.0.0.1:5173`. Axum still binds a random
loopback port and still enforces its exact Host, Origin, session, and CSRF
checks. There is no development authentication bypass.

Start Axum in the first terminal:

```bash
cargo run -p coding-agent-app
```

The process publishes `instance.json` in the runtime directory listed below.
Read its `port`, set the explicit proxy target, and start Vite in a second
terminal. On Windows PowerShell:

```powershell
$descriptor = Join-Path $env:LOCALAPPDATA 'ngy\coding-agent\data\run\instance.json'
$port = (Get-Content -LiteralPath $descriptor | ConvertFrom-Json).port
$env:CODING_AGENT_AXUM_TARGET = "http://127.0.0.1:$port"
npm --prefix web run dev
```

On Linux, first select the descriptor path and then start Vite:

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

On macOS:

```bash
descriptor="$HOME/Library/Application Support/com.ngy.coding-agent/run/instance.json"
port="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1], 'utf8')).port)" "$descriptor")"
export CODING_AGENT_AXUM_TARGET="http://127.0.0.1:$port"
npm --prefix web run dev
```

Once Vite is ready, run `cargo run -p coding-agent-app` once more. That short-
lived secondary process asks the primary for a fresh one-time URL and opens the
UI at the configured Vite origin. Keep the first Axum process running.

## Production build and direct launch

Build the React assets before compiling the embedded Rust artifact:

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

The result is `target/release/coding-agent-app` on Linux/macOS and
`target\release\coding-agent-app.exe` on Windows. It contains the React assets,
does not require Node or `web/dist` at runtime, accepts no normal CLI arguments,
and can be copied to and launched from any directory.

Launch it directly with `./target/release/coding-agent-app` on Linux/macOS or
`.\target\release\coding-agent-app.exe` in Windows PowerShell.

Starting a second copy does not create another database writer. It verifies the
existing primary, requests a fresh one-time URL, opens the browser, and exits.
The secondary does not read or validate `provider.json`.

## Provider configuration

Create `provider.json` in the data directory from the table below. It has one
strict schema; unknown fields are rejected:

```json
{
  "base_url": "https://provider.example/",
  "model": "provider-model-name",
  "api_key": "replace-with-the-provider-key"
}
```

The production base URL must use HTTPS and must not contain user information, a
query, or a fragment. The client sends Chat Completions requests to
`v1/chat/completions`, rejects redirects, uses a rustls TLS backend, and applies
bounded connect, request, response, and cumulative task limits. The API key is
8-4096 printable non-space ASCII bytes. The application uses the configured key
for provider authorization and boundary redaction; it does not copy the key
from `provider.json` into SQLite, model messages, child-process environments,
activity events, or ordinary logs. This is not general content scanning: task
prompts and retained repository artifacts are durable user data and may contain
credentials supplied by the user, so do not paste secrets into either one.

The file must be a regular, non-link private file. On Unix use mode `0600`, for
example `chmod 600 provider.json`. On Windows, ensure only the current user has
access; the application validates the opened file handle and rejects reparse
points or broad ACLs. Keep the data directory private as well.

## Data and runtime files

The application uses the host's per-user project directories:

| OS | Data directory | Runtime directory |
| --- | --- | --- |
| Windows | `%LOCALAPPDATA%\ngy\coding-agent\data` | `%LOCALAPPDATA%\ngy\coding-agent\data\run` |
| macOS | `~/Library/Application Support/com.ngy.coding-agent` | data directory plus `/run` |
| Linux | `${XDG_DATA_HOME:-~/.local/share}/coding-agent` | `$XDG_RUNTIME_DIR/coding-agent` when set; otherwise data directory plus `/run` |

`coding-agent.sqlite3` is the durable database. `instance.lock` and
`instance.json` coordinate the live process; the descriptor contains a private
launcher capability and must not be shared. `unclean-shutdown.json` records a
shutdown whose final task states could not be persisted and is recovered on the
next start.

Attempt artifacts are retained for inspection. Branches use
`codex/task-<task-id>-attempt-<attempt>` and worktrees are stored under the
private data directory at
`worktrees/<repository-id>/<task-id>/<attempt>`. SQLite records the immutable
repository/task/attempt identity, base commit, branch, worktree path, and
`reserved`, `ready`, or `inconsistent` lifecycle state. A retry always receives
a new branch and worktree; the application never overwrites or deletes an
unknown conflicting artifact.

For a main-file-only backup, quit through the Web UI and confirm a clean
shutdown: the process and `instance.json` are gone, no degraded-shutdown warning
was shown, `unclean-shutdown.json` is absent, and no non-empty
`coding-agent.sqlite3-wal` remains. Only then copy `coding-agent.sqlite3`. If
shutdown was degraded or a recovery marker/WAL remains, preserve the complete
data directory, relaunch the application, wait for recovery to reach Connected,
and quit cleanly before taking the main-file-only backup. Do not copy or replace
the database while the application is running; use SQLite-aware recovery tools
if a clean shutdown cannot be obtained. Restore only while the application is
stopped and keep the backup with the application version that created it.

## Exiting and recovering the browser

Use the application menu in the Web UI and choose **Quit local application**.
The process closes its mutation gate, settles in-flight work, persists
interrupted tasks, attempts the final SQLite checkpoint, removes the descriptor,
and releases the single-instance lock. A degraded warning or
`unclean-shutdown.json` means that the normal persistence/checkpoint path did
not finish cleanly. Closing a tab or the whole browser does **not** stop tasks or
the application.

If automatic browser opening fails, copy the complete one-time URL from the
native error dialog and open it manually. If the tab was lost or that URL
expired, start the executable again; the secondary process asks the verified
primary for a fresh one-time URL. The primary remains alive after an ordinary
browser-open failure.

## Security boundary

The server listens only on a random IPv4 loopback port. Exact Host checks,
one-time launch exchange, process-scoped session cookies, exact Origin checks,
and CSRF tokens protect ordinary cross-site browser access. Launcher and
provider-configuration secrets are kept out of URLs after exchange and ordinary
logs, and the application does not copy the configured provider key into
SQLite. Task prompts and retained artifacts remain durable user data and can
contain secrets supplied as task or repository content.

Git worktrees and capability-based file tools isolate an attempt from the
user's original working directory, but Project 2 is **not** an OS sandbox for
untrusted code. Cargo may execute an existing or generated `build.rs`, proc
macro, dependency, test binary, and other repository code with the current OS
user's permissions. That code can attempt to read or write outside the
worktree, access the network, or start processes. Run tasks only for repositories
and generated changes you are willing to execute as the current user; use a
separately hardened VM/container for genuinely untrusted code.

Final test evidence is bound to the actual terminal workspace fingerprint.
Diff collection also checks the workspace before and after collection, but it
does not provide a filesystem snapshot or linearizability against a malicious
same-user process deliberately changing bytes and restoring them during that
window. The retained worktree remains the authoritative artifact in that
out-of-boundary scenario.

The built-in tools reject path escape, `.git` access, links/reparse points,
unbounded output, inherited secrets, arbitrary commands, and executable Git
configuration. Cargo runs offline by default and child process trees are
terminated on timeout or cancellation. These controls reduce accidental tool
escape; they do not change the trusted-code boundary above. A malicious process
already running as the same OS user can also inspect that user's memory, files,
browser traffic, or local endpoints. Repository paths and the runtime descriptor
should be treated as private user data.

## Troubleshooting codes

Errors shown by the UI include a stable code and request ID. Keep both when
matching an error to local logs.

| Code | Meaning and action |
| --- | --- |
| `APP_STARTING` | Startup recovery is still running. Wait briefly, then launch the executable again. |
| `APP_SHUTTING_DOWN` | Shutdown has closed the mutation gate. Wait for exit before relaunching. |
| `STORE_BUSY` | SQLite could not accept a write within the foreground budget. Use the UI's explicit retry; create retries retain their request UUID. |
| `STORE_DEGRADED` | Mutations and new claims are paused while durable recovery runs. Wait for the Connected banner; restart only if recovery does not converge. |
| `APP_RESTARTED` | An incomplete task was interrupted during crash/restart recovery. Retry it explicitly if desired. |
| `APP_SHUTDOWN` | An incomplete task was interrupted by an orderly application quit. |
| `STORE_WRITE_FAILED` | A task had an ambiguous background write and was durably interrupted during recovery. |
| `PROVIDER_CONFIG_INVALID` | Create a strict, private `provider.json`; verify HTTPS, field names, file permissions, model, and API key. Production never falls back to a fake runner. |
| `PROVIDER_UNAUTHORIZED` | Verify the configured API key and provider account without placing the key in logs or issue reports. |
| `PROVIDER_RATE_LIMITED` / `PROVIDER_UNAVAILABLE` / `PROVIDER_TRANSPORT_FAILED` | The provider throttled, failed, disconnected, or timed out. Retry after the provider is healthy. |
| `PROVIDER_REQUEST_BYTE_LIMIT_REACHED` / `PROVIDER_TASK_BYTE_LIMIT_REACHED` | The provider exchange exceeded its per-request or cumulative task budget. Narrow the task and retry. |
| `PROVIDER_RESPONSE_INVALID` / `PROVIDER_UNSUPPORTED_MULTIPLE_TOOL_CALLS` / `PROVIDER_REDIRECT_REJECTED` | The endpoint did not honor the supported single-tool-call Chat Completions contract. Check provider compatibility and base URL. |
| `GIT_HEAD_UNBORN` | Commit an initial repository revision before creating a task. Dirty working-directory bytes are intentionally not used. |
| `WORKTREE_CREATE_FAILED` / `WORKTREE_STATE_INCONSISTENT` | Inspect the retained attempt artifact. Resolve branch/path identity conflicts manually; the application does not delete unknown objects. |
| `WORKTREE_PATH_ESCAPE` | A reserved attempt path or Cargo workspace escaped its trusted repository/artifact roots. Inspect the registered repository and retained artifact instead of weakening the boundary. |
| `FILE_NOT_TEXT` / `FILE_TOO_LARGE` / `FILE_CHANGED_SINCE_READ` / `ATOMIC_REPLACE_FAILED` | The requested file cannot be safely read or atomically replaced. Retry only after checking the retained worktree state. |
| `COMMAND_NOT_ALLOWED` / `COMMAND_TIMED_OUT` / `PROCESS_TREE_CLEANUP_FAILED` | A command or `.git` path violated the fixed tool contract, timed out, or could not be safely quiesced. Bounded output is returned as explicitly truncated tool evidence. Inspect the worktree and terminate any suspect repository process before retrying. |
| `CARGO_METADATA_FAILED` / `CARGO_DEPENDENCY_UNAVAILABLE_OFFLINE` | Fix the Cargo workspace or prefetch its dependencies explicitly; task Cargo commands do not fetch from the network. |
| `AGENT_STEP_LIMIT_REACHED` / `AGENT_CONTEXT_LIMIT_REACHED` | The bounded agent loop exhausted its step or context budget before a valid final result. Narrow the task and retry. |
| `CURRENT_TEST_REQUIRED` | The final worktree fingerprint has no passing Cargo test evidence. Run the relevant test after the last source change. |
| `TERMINAL_DIFF_TRUNCATED` | The retained final diff exceeded a safety bound, so the task was not marked Completed. Inspect the worktree and split the change into a smaller task. |
| `TERMINAL_FINALIZATION_TIMEOUT` | Final test/diff evidence did not settle within the bounded terminal window. Inspect the retained worktree and retry only after any repository process has stopped. |
| `TOOLCHAIN_DISCOVERY_FAILED` | Startup could not pin the required Git/Rust tools. Verify Git 2.45+ and the active Rust toolchain, then relaunch. |
| `RUNNER_PANICKED` | The runner failed unexpectedly; the task is isolated and other work remains available. Preserve the request ID and retained artifact for diagnosis. |
| `REPOSITORY_PATH_NOT_FOUND` / `REPOSITORY_PATH_NOT_DIRECTORY` | Select an existing directory. |
| `CARGO_WORKSPACE_NOT_FOUND` / `CARGO_WORKSPACE_OUTSIDE_GIT_ROOT` | Select a Cargo workspace contained by its Git repository. |
| `REPOSITORY_COMMAND_FAILED` | Verify that Git and Cargo are installed and the repository can be inspected. |
| `PICKER_ALREADY_OPEN` / `PICKER_UNAVAILABLE` | Finish the current picker, or enter the repository path directly. |
| `SECURITY_INVALID_SESSION` | The process restarted or the session expired. Launch the executable again for a fresh one-time URL. |
| `SECURITY_INVALID_HOST` / `SECURITY_INVALID_ORIGIN` / `SECURITY_INVALID_CSRF` | The request did not come from the current application page. Reopen the executable instead of weakening the checks. |
| `SECURITY_INVALID_LAUNCHER_SECRET` / `SECURITY_INVALID_LAUNCH_TOKEN` / `SECURITY_DUPLICATE_HEADER` | A launcher capability was stale, invalid, replayed, or ambiguous. Discard the URL and launch the executable again. |
| `INVALID_PROMPT` / `INVALID_REPOSITORY_PATH` | Correct the task text or repository path shown by the UI. |
| `IDEMPOTENCY_CONFLICT` | The same request UUID was reused with different content. Retry from the UI as a new operation. |
| `TASK_NOT_FOUND` / `TASK_NOT_RETRYABLE` / `TASK_NOT_CANCELLABLE` | Refresh task state; the task is missing or its durable status no longer permits that action. |
| `NETWORK_ERROR` / `INTERNAL_ERROR` | Reopen the application and retry once; keep the request ID and logs if the error repeats. |
| `SHUTDOWN_PERSISTENCE_FAILED` | The process exits nonzero and leaves recovery state for the next start; preserve the data directory and relaunch. |
