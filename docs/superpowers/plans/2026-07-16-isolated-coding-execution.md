# Isolated Coding Execution Implementation Plan

> Execution rule: work task-by-task in order using TDD. Do not begin Project 3 behavior while executing this plan.

**Goal:** Replace the production fake runner with a globally single-concurrency coding runner that creates a per-attempt Git worktree, drives an OpenAI-compatible single-role tool loop, safely reads/replaces files, runs bounded typed Cargo/Git operations, and persists truthful plan/activity/diff/test events through the existing Project 1 lifecycle.

**Architecture:** Add `coding-agent-core` for provider/runtime ports and the deterministic agent loop, `coding-agent-provider` for the locked HTTP subset, and `coding-agent-runtime` for Git/file/process/Cargo capabilities. `coding-agent-app` remains the adapter and composition root: it maps neutral core events to `RunnerEvent`, serializes attempt-artifact mutations through `StoreWriter`, and alone implements `TaskRunner`.

**Tech stack:** Rust 1.97, edition 2024, Tokio, Axum loopback mock servers, rustls-backed HTTPS, SHA-256, SQLite/SQLx, Unix process groups, Windows Job Objects, React/TypeScript/Vite.

## Global constraints

- Source specification: `docs/superpowers/specs/2026-07-16-isolated-coding-execution-design.md`.
- Use a `codex/` implementation branch and preserve unrelated user changes.
- For every behavior: add the focused failing test, observe the intended failure, add the minimum implementation, rerun focused and impacted suites, then inspect the diff.
- Dependency graph is acyclic: `app -> {core,provider,runtime}` and `{provider,runtime} -> core`; core never depends on app.
- Runtime SQLite mutations go through `StoreWriter`; runners never write `Store` directly.
- First-party file targets and command cwd stay within the worktree. Original and model-generated code executed by Cargo is trusted and receives current-user authority; do not claim OS sandboxing, and expose this warning in the product UI/docs.
- All `.git` metadata is hidden and protected. Git hooks and executable filters/config must not run.
- The model receives typed tools only; it never supplies an arbitrary executable, argv, cwd, Git path option, Cargo manifest/target/config path, or shell string.
- Production real-runner concurrency is exactly 1. Test fake concurrency may remain 4; bootstrap reports the actual selection.
- Provider, command, diff, and tool output are bounded and redacted before tracing, SQLite, or model context.
- Default tests are offline and use only loopback mock HTTP.
- `Completed` still means runner success only, never reviewed, deliverable, or mergeable.

## Locked ownership map

```text
Cargo.toml / Cargo.lock
crates/coding-agent-core/
  src/{lib,error,limits,model,ports,agent,tools,conversation,budget,event}.rs
  tests/{ports,agent_loop,budgets,cancellation}.rs
crates/coding-agent-provider/
  src/{lib,config,dto,error,redaction,client}.rs
  tests/{schema,redaction,contract}.rs
crates/coding-agent-runtime/
  src/{lib,relative_path,root_capability,files,search,replace}.rs
  src/{command_policy,environment,output,cargo,diff,runtime_adapter}.rs
  src/git/{mod,worktree}.rs
  src/process/{mod,unix,windows}.rs
  tests/* plus tests/support/mod.rs
crates/coding-agent-store/
  migrations/0002_task_attempt_artifacts.sql
  src/attempt_artifacts.rs
  tests/attempt_artifacts.rs
crates/coding-agent-app/
  src/{coding_agent_runner,provider_config}.rs
  src/{store_writer,single_instance,main,test_support,lib}.rs
  tests/{coding_agent_runner,provider_config,isolated_coding_e2e}.rs
web/src/ and README.md
```

Names may be coalesced when a smaller module is clearer, but ownership and dependency direction are locked.

## Task 1: Establish core ports and crate graph

- [x] Add a failing `coding-agent-core/tests/ports.rs` contract for provider messages, exactly one tool call, async `ModelProvider`/`ToolRuntime` ports, cancellation, neutral runner events, and validated non-zero `AgentLimits`.
- [x] Run `cargo test -p coding-agent-core --test ports` and observe the missing crate/API failure.
- [x] Add the three workspace crates and minimal core DTO/trait implementation. Provider/runtime roots only compile their intended dependency edge.
- [x] Verify:

```powershell
cargo test -p coding-agent-core --test ports
cargo check --workspace --all-targets
cargo fmt --all --check
```

Checkpoint: no new crate depends on `coding-agent-app`.

## Task 2: Parse capability-safe relative paths

- [x] Add failing runtime tests for absolute/UNC/device paths, `.`/`..`, NUL, Windows ADS, platform-equivalent `.git` names, non-UTF-8 input, symlink/junction/reparse traversal, and ancestor-swap races.
- [x] Implement `RelativePath` as a logical path without authority.
- [x] Implement `RootCapability` using handle/fd-relative per-component no-follow traversal. Writes fail closed if the platform cannot establish the required semantics.
- [x] Keep unsafe platform code narrow and document invariants.
- [x] Run `cargo test -p coding-agent-runtime --test path_security -- --nocapture` and clippy for the crate.

## Task 3: Implement bounded read/list/search

- [x] Test line ranges, byte truncation, binary rejection, depth/item caps, literal search/glob validation, protected `.git`, default `target` exclusion, and concurrent ancestor replacement.
- [x] Implement handle-relative tools without depending on external `rg`.
- [x] Return structured truncation/count metadata and stable codes.
- [x] Run `cargo test -p coding-agent-runtime --test file_tools` plus `path_security`.

## Task 4: Implement digest-bound atomic replacement

- [x] Test create with `expected_sha256=null`, existing-file digest match, stale/missing conflicts, permissions, same-directory exclusive temp, flush, cancel-before-publication cleanup, ancestor races, and Windows occupied-target preservation.
- [x] Implement capability-relative temp creation, `sync_all`, pre-publication revalidation and platform atomic publication.
- [x] Never degrade to truncate-in-place or copy/delete; publication success is committed even if cancellation is observed immediately afterward.
- [x] Run `cargo test -p coding-agent-runtime --test atomic_replace -- --nocapture` and path tests.

## Task 5: Supervise bounded process trees

- [x] Test separate stdout/stderr draining, head/tail truncation, dual-pipe flood, non-zero exit, wall timeout, cancellation precedence, leader-exits/grandchild-survives, future abort/drop, and bounded cleanup.
- [x] Implement `env_clear()` with an explicit platform/toolchain environment builder and prove provider/proxy/SSH/CI secrets are absent.
- [x] Unix: independent process group with kill/wait. Windows: pre-user-code Job assignment with `KILL_ON_JOB_CLOSE` using job-list attribute or suspended-create/assign/resume.
- [x] Ensure normal leader exit still cleans descendants before bounded pipe completion; RAII drop kills too.
- [x] Run `bounded_output`, `environment`, and `process_tree` tests on the current platform.

## Task 6: Lock typed command policy and Cargo adapters

- [x] Test that only internal `ValidatedCommand` constructors exist and reject shell, arbitrary executable/argv/cwd, Git mutation/path options and Cargo manifest/target/config injection.
- [x] Test pinned tool discovery and offline metadata/check/test with package/test names restricted to trusted metadata, truthful status, timeout and cancellation.
- [x] Implement typed model tools only: Cargo metadata/check/test plus Git status/diff; no generic `run_command`.
- [x] Run `command_policy` and `cargo_tools` tests.

## Task 7: Provision and validate Git worktrees

- [x] Use temporary real repos to test unborn HEAD, dirty/staged/untracked isolation, nested workspace mapping, unique branch/path, retry isolation, conflicts and all creation crash points.
- [x] Prove post-checkout hooks, executable filters, fsmonitor, external diff and textconv never execute; reject unsafe repo configuration where necessary.
- [x] Create from committed HEAD with deterministic application-owned identities and bind later Git operations to validated git-dir/work-tree values.
- [x] Never derive authority from the model-hidden linked-worktree `.git` file.
- [x] Run `cargo test -p coding-agent-runtime --test worktree -- --nocapture`.

## Task 8: Collect bounded diffs

- [x] Test added/modified/deleted files, binary and non-UTF-8 paths, counts, deterministic order, patch caps, and disabled external diff/textconv.
- [x] Implement neutral diff DTOs and the concrete runtime adapter.
- [x] Run `cargo test -p coding-agent-runtime --test diff` and the full runtime crate. Focused diff tests pass; the managed Windows full-crate run passes 57/60 unit tests, with three pre-existing `atomic_replace` tests blocked by filesystem `PermissionDenied`.

## Task 9: Persist artifact lifecycle through StoreWriter

- [x] Add migration v2 tests for old-database upgrade, repeat migration, exact constraints and rollback.
- [x] Test task/repository/attempt identity, unique branch/path, `reserved -> ready|inconsistent`, identical idempotency and conflict rejection. Add composite DB identity constraints.
- [x] Extend `StoreWriter` with reserve/ready/inconsistent operations; do not expose direct runner writes.
- [x] Test startup reconciliation for reserved+absent, reserved+valid, partial and mismatched Git/disk state, distinguishing same-run reentry from restart-abandoned state.
- [x] Run store migration/artifact tests and app StoreWriter tests.

## Task 10: Validate provider config, schema, errors and redaction

- [x] Test strict `provider.json` schema, private permissions, HTTPS-only remote URLs, test-only loopback HTTP, forbidden userinfo/query/fragment, and secret-safe Debug/Display.
- [x] Test messages, single `tool_call_id` round trip, multiple-call rejection, unknown/oversized response rejection and retryable error mapping.
- [x] Implement redaction before any log or user boundary.
- [x] Run provider schema/redaction and app provider-config tests.

## Task 11: Implement HTTPS provider contract

- [x] Use a local Axum server to test exact POST path/body/tools/tool_choice/Bearer behavior, timeouts, 401/429/5xx, disconnect, malformed body, no-length chunk flood, oversized JSON, compression bomb, rejected 30x and request ID.
- [x] Add a rustls-backed client; production HTTPS cannot rely on an absent native TLS setup.
- [x] Assert no test contacts an unconfigured or real provider.
- [x] Run `cargo test -p coding-agent-provider --test contract -- --nocapture` and the provider crate.

## Task 12: Implement deterministic single-role loop

- [x] Script ports to test tool call → result → continuation → final text, invalid calls, retryable/fatal errors, budgets, cancellation priority and terminal snapshot collection.
- [x] Test workspace revision and fingerprint: tracked plus non-ignored untracked content is included, ignored `target/` output is excluded, hashing is streaming/deterministic, every count/byte cap fails closed, replace increments and queues invalidation, start/end/final fingerprints bind tests, test code or an external process changing source invalidates pass, and only a current fingerprint permits success.
- [x] Keep context bounded and never persist chain-of-thought or raw provider bodies.
- [x] Emit neutral events only; app performs domain mapping.
- [x] Run core agent-loop, budget and cancellation tests.

## Task 13: Adapt CodingAgentRunner

- [x] Test reserve/provision/ready, initial plan/activity, event mapping, debounced/terminal diff, running/terminal tests, normal cancellation, sink rejection, stable failure mapping and retention.
- [x] Add forced-quiesce regression: Interrupted may retain the latest durable diff and late events remain rejected.
- [x] Implement the app adapter using StoreWriter-backed artifacts and existing `RunContext` cancellation.
- [x] Leave terminal task transitions exclusively in `TaskManager` via `RunnerOutcome`.
- [x] Run coding-runner, task-manager and shutdown tests.

## Task 14: Select production runner and concurrency after primary lock

- [x] Test that secondary launch ignores provider config, primary requires valid private config, invalid/missing config never falls back to fake, real reports concurrency 1, and injected fake reports its configured concurrency.
- [x] Replace fixed startup runner with a factory invoked only after private paths and primary lock; return `{ runner, concurrency }`.
- [x] Keep fake/mock explicit in tests; update release smoke with a valid non-contacted config and concurrency 1 assertion.
- [x] Run single-instance, process-support and release-smoke tests.

## Task 15: Prove offline E2E and update product surface

- [x] Script loopback provider over a temporary dirty Rust repo: read → replace → Cargo test → final.
- [x] Assert Completed, artifact/branch/worktree identity, original staged/unstaged/untracked bytes unchanged, current-revision pass, bounded diff, SQLite projection and SSE replay.
- [x] Add failure E2Es for disconnect, test failure, timeout, cancellation, output flood, replace-after-pass, path escape and restart interruption.
- [x] Update UI copy, README config/threat-model/artifact/troubleshooting documentation, and CI where platform gates need explicit coverage.
- [x] Run frontend checks plus full formatting, clippy, workspace tests and `git diff --check`.

## Final review and acceptance

- [x] Independent review focuses on path races, `.git`/Git config execution, Windows pre-execution Job assignment, StoreWriter ordering, redaction and revision-bound tests.
- [x] Resolve every blocker/high finding and rerun impacted tests.
- [x] Capture fresh three-platform CI evidence.
- [x] Demonstrate success plus timeout, cancellation, path escape, malicious Git config and replace-after-pass failures.
- [x] Record exact commands/results and verify no secrets, generated drift or placeholders.

Fresh three-platform evidence is GitHub Actions run
[`29738805404`](https://github.com/taoistwar/ngy-coding-agent/actions/runs/29738805404)
for commit `f0abaa24e50e9583a27227a6457f7f180b323c00`:

- Linux quality and E2E job `88340446147` — success, including browser E2E and the embedded release build.
- Ubuntu release-smoke job `88340446186` — success.
- Windows release-smoke job `88340446187` — success.
- macOS release-smoke job `88340446212` — success, including the full workspace tests that exercise Darwin process-tree cleanup and concurrent worktree fixtures.

### Fresh acceptance evidence — Windows and GitHub CI, 2026-07-20

- `cargo fmt --all -- --check` and `git diff --check` — passed.
- `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` — passed.
- `cargo test --workspace --all-features --locked` — passed locally in 514.7 seconds after the final production fixes; the fresh CI run repeated the full workspace on Linux, Ubuntu, Windows and macOS.
- `cargo test -p coding-agent-app --test task_manager --all-features --locked --offline` — 27/27 passed; the FIFO claim-order regression then passed 50/50 consecutive runs while observing the runner's actual start sequence.
- Focused runtime integration tests for worktree, diff, fingerprint, typed Git and typed Cargo — 23/23 passed; app artifact reconciliation and real offline E2E — 8/8 passed.
- `cargo test -p coding-agent-runtime -p coding-agent-app --lib --all-features --locked --offline` — 151/151 passed, including the production runner factory with concurrency one and process-supervisor cleanup tests.
- The CI jobs also passed generated API drift checks, frontend type-check/tests/build, placeholder rejection, embedded release builds and release application startup smoke tests without contacting a real provider.
- Independent audits approved the Darwin/XNU quiescent-group handling, actual FIFO start-order synchronization and process-global spawn-lock coverage; no blocker or high finding remains.
