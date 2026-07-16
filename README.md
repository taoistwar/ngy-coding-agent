# ngy Coding Agent

This repository contains Project 1 of a local, browser-based Coding Agent. The
application is a Rust process that owns Axum, SQLite, task orchestration, and
native dialogs, with a React UI served on a random `127.0.0.1` port.

## Project 1 scope

Project 1 is a deterministic fake platform used to prove the local application
architecture and lifecycle. It can register real Git/Cargo repositories and
exercise realistic task, activity, diff, test, retry, recovery, and shutdown UI
states. Its task runner does **not** read or modify repository source, call a
model, create worktrees, execute repository tests, review changes, merge work,
or imply that a task is deliverable.

Installers, a macOS application bundle, Linux desktop entries, code signing and
notarization, auto-update, and polished launcher packaging belong to Project 4.
They are not Project 1 CI gates.

## Prerequisites

- Rust `1.97.0` with `rustfmt` and `clippy` (pinned by
  `rust-toolchain.toml`).
- Node.js 24 or newer and npm.
- Git and Cargo available on `PATH` for repository discovery.
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
and CSRF tokens protect ordinary cross-site browser access. Secrets are kept
out of URLs after exchange, SQLite, and normal logs.

This is not a sandbox against a malicious process already running as the same
OS user. Such a process can inspect that user's memory, files, browser traffic,
or local endpoints. Repository paths and the runtime descriptor should be
treated as private user data.

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
| `RUNNER_PANICKED` / `FAKE_RUNNER_FAILURE` | The deterministic Project 1 runner reached an injected panic or failure scenario; other tasks remain isolated. |
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
