import { execFile, spawn, type ChildProcessByStdio } from "node:child_process";
import { randomUUID } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import {
  access,
  link,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readFile,
  realpath,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { Readable } from "node:stream";
import { promisify } from "node:util";

import type { Page, TestInfo } from "@playwright/test";

const execFileAsync = promisify(execFile);
const START_TIMEOUT_MS = 20_000;
const CLEAN_EXIT_TIMEOUT_MS = 10_000;
const GRACEFUL_STOP_TIMEOUT_MS = 5_000;
const FORCED_STOP_TIMEOUT_MS = 5_000;
const MAX_DESCRIPTOR_BYTES = 4 * 1024;
const MAX_HTTP_RESPONSE_BYTES = 64 * 1024;
const MAX_LOG_BYTES = 512 * 1024;
const LOG_TRUNCATION_MARKER = "[earlier process output omitted]\n";
const UTC_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z$/u;

const TEST_APP_DATA_ENV = "CODING_AGENT_TEST_APP_DATA_DIR";
const TEST_RUNTIME_ENV = "CODING_AGENT_TEST_RUNTIME_DIR";
const TEST_SCENARIO_ENV = "CODING_AGENT_TEST_SCENARIO";
const REACHED_SIGNAL_TIMEOUT_MS = 10_000;
const SIGNAL_NAME = /^[A-Za-z0-9._-]{1,64}$/u;
const ADDITIONAL_REPOSITORY_NAME = /^[a-z][a-z0-9-]{0,47}$/u;
const WINDOWS_RESERVED_NAME = /^(?:con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/iu;
const reachedSignalBrand: unique symbol = Symbol("coding-agent-e2e-reached-signal");
type AppProcess = ChildProcessByStdio<null, Readable, Readable>;

export type FakeScenario =
  | "success"
  | "multi_role_approved"
  | "multi_role_rework_approved"
  | "multi_role_rejected"
  | "blocking"
  | "ignores_cancellation"
  | "failure"
  | "panic";

export type StoreWriterFaultPoint =
  | "fail_before_execute"
  | "fail_unknown_before_execute"
  | "busy_before_execute"
  | "pause_before_execute"
  | "pause_after_commit_before_wake"
  | "drop_wake_after_commit";

export type StoreWriterOperation =
  | "register_repository"
  | "create_task"
  | "retry_task"
  | "persist_stop_intent_batch"
  | "finalize_stopped_task"
  | "start_task"
  | "reconcile_claim_task"
  | "finish_task"
  | "cancel_task"
  | "interrupt_task"
  | "append_running_event"
  | "record_review"
  | "finalize_reviewed_task"
  | "reserve_attempt_artifact"
  | "mark_attempt_artifact_ready"
  | "mark_attempt_artifact_inconsistent"
  | "interrupt_remaining_after_stops"
  | "recover_incomplete";

export type ActorPausePoint =
  | "cancel_enqueued"
  | "claim_permit_acquired"
  | "claim_handle_registered"
  | "claim_running_committed"
  | "after_final_gate_before_spawn"
  | "terminal_after_dispatch_before_scheduler_publish"
  | "create_before_write"
  | "retry_before_write"
  | "result_before_write"
  | "quiesce_before_recovery"
  | "recovery_before_descriptor"
  | "descriptor_before_browser"
  | "task_detail_after_snapshot"
  | "bootstrap_before_sse"
  | "bootstrap_cursor_ahead";

export type VirtualReleaseTarget =
  | "runner_next"
  | "storage_next"
  | "store_writer_before_execute"
  | "store_writer_after_commit_before_wake"
  | "actor_cancel_enqueued"
  | "actor_claim_permit_acquired"
  | "actor_claim_handle_registered"
  | "actor_claim_running_committed"
  | "actor_after_final_gate_before_spawn"
  | "actor_terminal_after_dispatch_before_scheduler_publish"
  | "actor_create_before_write"
  | "actor_retry_before_write"
  | "actor_result_before_write"
  | "actor_quiesce_before_recovery"
  | "actor_recovery_before_descriptor"
  | "actor_descriptor_before_browser"
  | "actor_task_detail_after_snapshot"
  | "actor_bootstrap_before_sse"
  | "actor_bootstrap_cursor_ahead";

export type LegacyV2Seed =
  | { kind: "none" }
  | {
      kind: "completed_task";
      repository_path: string;
      task_prompt: string;
    };

export type ProcessStorageSample =
  | { kind: "native" }
  | { kind: "available"; available_bytes: number }
  | { kind: "unavailable" };

export interface ProcessRuntimeConfig {
  schema_version: 1;
  max_concurrent_tasks: number;
  max_concurrent_tasks_per_repository: number;
  max_queued_tasks: number;
  storage: {
    data_control_reserve_bytes: number;
    data_task_reservation_bytes: number;
  };
}

export interface ProcessScenario {
  runtime_config: ProcessRuntimeConfig | null;
  fake_scenarios: FakeScenario[];
  storage_samples: ProcessStorageSample[];
  store_writer_faults: Array<{
    point: StoreWriterFaultPoint;
    operation: StoreWriterOperation | null;
    count: number;
  }>;
  actor_pauses: ActorPausePoint[];
  virtual_release_signals: Array<{
    name: string;
    path: string;
    target: VirtualReleaseTarget;
  }>;
  legacy_v2_seed: LegacyV2Seed;
  marker_write_failure: boolean;
}

export interface ScenarioRoots {
  readonly root: string;
  readonly appDataDir: string;
  readonly runtimeDir: string;
  readonly repositoryDir: string;
  releaseSignalPath(name: string): string;
}

export type ScenarioFactory = (
  roots: ScenarioRoots,
) => ProcessScenario | Promise<ProcessScenario>;

export type ScenarioSource = ProcessScenario | ScenarioFactory;

export interface ReachedSignal {
  readonly runtimeDir: string;
  readonly releasePath: string;
  readonly reachedPath: string;
  readonly [reachedSignalBrand]: true;
}

export interface RepositorySnapshot {
  trackedFiles: string[];
  lockfile: string;
  dirtyStatus: string;
}

export const successScenario = (): ProcessScenario => ({
  runtime_config: null,
  fake_scenarios: ["success"],
  storage_samples: [{ kind: "native" }],
  store_writer_faults: [],
  actor_pauses: [],
  virtual_release_signals: [],
  legacy_v2_seed: { kind: "none" },
  marker_write_failure: false,
});

export function isValidReleaseSignalName(name: string): boolean {
  return SIGNAL_NAME.test(name) && !WINDOWS_RESERVED_NAME.test(name);
}

interface RuntimeDescriptor {
  instance_id: string;
  pid: number;
  port: number;
  started_at: string;
  launcher_secret: string;
}

interface ReopenGrant {
  url: string;
  expires_at: string;
}

interface PrimaryGeneration {
  child: AppProcess;
  descriptor: RuntimeDescriptor;
  initialLaunchUrl: string | null;
  stdout: BoundedLog;
  stderr: BoundedLog;
}

interface ProcessRecord {
  child: AppProcess;
  stdout: BoundedLog;
  stderr: BoundedLog;
  role: "primary" | "secondary";
}

export interface RuntimeIdentity {
  readonly instanceId: string;
  readonly pid: number;
  readonly port: number;
  readonly startedAt: string;
}

export interface SecondaryExit {
  readonly pid: number;
  readonly exitCode: number;
  readonly signalCode: NodeJS.Signals | null;
}

export class LocalApp {
  readonly root: string;
  readonly appDataDir: string;
  readonly runtimeDir: string;
  readonly repositoryDir: string;
  readonly descriptorPath: string;

  private readonly absoluteBinary: string;
  private readonly roots: ScenarioRoots;
  private active: PrimaryGeneration;
  private readonly processes: ProcessRecord[];
  private readonly secrets = new Set<string>();
  private stopPromise: Promise<void> | null = null;

  constructor(options: {
    root: string;
    appDataDir: string;
    runtimeDir: string;
    repositoryDir: string;
    descriptorPath: string;
    descriptor: RuntimeDescriptor;
    launchUrl: string;
    child: AppProcess;
    stdout: BoundedLog;
    stderr: BoundedLog;
    absoluteBinary: string;
    roots: ScenarioRoots;
  }) {
    this.root = options.root;
    this.appDataDir = options.appDataDir;
    this.runtimeDir = options.runtimeDir;
    this.repositoryDir = options.repositoryDir;
    this.descriptorPath = options.descriptorPath;
    this.absoluteBinary = options.absoluteBinary;
    this.roots = options.roots;
    this.active = {
      child: options.child,
      descriptor: options.descriptor,
      initialLaunchUrl: options.launchUrl,
      stdout: options.stdout,
      stderr: options.stderr,
    };
    this.processes = [
      {
        child: options.child,
        stdout: options.stdout,
        stderr: options.stderr,
        role: "primary",
      },
    ];
    this.rememberGrant(options.launchUrl);
    this.secrets.add(options.descriptor.launcher_secret);
  }

  get origin(): string {
    return `http://127.0.0.1:${this.active.descriptor.port}`;
  }

  get pid(): number {
    return this.active.descriptor.pid;
  }

  get port(): number {
    return this.active.descriptor.port;
  }

  async open(page: Page): Promise<void> {
    const launchUrl = this.active.initialLaunchUrl;
    if (launchUrl === null) {
      throw new Error("the initial one-time launch grant has already been used");
    }
    this.active.initialLaunchUrl = null;
    await this.navigateToGrant(page, launchUrl);
  }

  async reopen(page: Page): Promise<void> {
    const grant = await requestReopenGrant(this.active.descriptor);
    const launchUrl = validateReopenGrant(grant, this.active.descriptor.port);
    this.rememberGrant(launchUrl);
    await this.navigateToGrant(page, launchUrl);
  }

  async runtimeIdentity(): Promise<RuntimeIdentity> {
    const descriptor = await readRuntimeDescriptor(this.descriptorPath);
    return Object.freeze({
      instanceId: descriptor.instance_id,
      pid: descriptor.pid,
      port: descriptor.port,
      startedAt: descriptor.started_at,
    });
  }

  async hardKillPrimaryPreservingRoot(timeoutMs = FORCED_STOP_TIMEOUT_MS): Promise<void> {
    this.assertFixtureOpen("hard-kill the primary");
    await hardKillProcess(this.active.child, timeoutMs).catch((error: unknown) => {
      throw new Error(
        this.redact(`could not hard-kill coding-agent: ${errorText(error)}\n\n${this.diagnostics()}`),
      );
    });
  }

  async restart(scenarioSource: ScenarioSource = successScenario()): Promise<void> {
    this.assertFixtureOpen("restart the primary");
    if (processIsRunning(this.active.child)) {
      throw new Error("the active primary must exit before restart");
    }
    const previousDescriptor = this.active.descriptor;
    const generation = await spawnPrimaryGeneration({
      absoluteBinary: this.absoluteBinary,
      roots: this.roots,
      descriptorPath: this.descriptorPath,
      scenarioSource,
      previousInstanceId: previousDescriptor.instance_id,
    }).catch((error: unknown) => {
      throw new Error(this.redact(`could not restart coding-agent: ${errorText(error)}`));
    });
    this.active = generation;
    this.processes.push({
      child: generation.child,
      stdout: generation.stdout,
      stderr: generation.stderr,
      role: "primary",
    });
    this.secrets.add(generation.descriptor.launcher_secret);
    if (generation.initialLaunchUrl !== null) this.rememberGrant(generation.initialLaunchUrl);
  }

  async startSecondaryAndWait(
    scenarioSource: ScenarioSource,
    expectedExitCode = 0,
    timeoutMs = CLEAN_EXIT_TIMEOUT_MS,
  ): Promise<SecondaryExit> {
    this.assertFixtureOpen("start a secondary process");
    if (!processIsRunning(this.active.child)) {
      throw new Error("a secondary process requires a running primary");
    }
    if (!Number.isInteger(expectedExitCode)) {
      throw new Error("expected secondary exit code must be an integer");
    }
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      throw new Error("secondary timeout must be a positive finite number");
    }

    const scenarioPath = await writeFreshScenario(this.roots, scenarioSource);
    const stdout = new BoundedLog();
    const stderr = new BoundedLog();
    const child = spawnConfiguredProcess(
      this.absoluteBinary,
      this.root,
      this.appDataDir,
      this.runtimeDir,
      scenarioPath,
    );
    const record: ProcessRecord = { child, stdout, stderr, role: "secondary" };
    this.processes.push(record);
    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));

    try {
      const deadline = Date.now() + timeoutMs;
      await waitForScenarioConsumed(
        scenarioPath,
        child,
        remainingMilliseconds(deadline),
      );
      const exited = await waitForPromise(
        waitForExit(child),
        remainingMilliseconds(deadline),
      );
      if (!exited) {
        throw new Error(`secondary process did not exit within ${timeoutMs} ms`);
      }
      const exitCode = child.exitCode;
      const signalCode = child.signalCode;
      if (exitCode !== expectedExitCode || signalCode !== null) {
        throw new Error(
          `secondary process exit mismatch (expected=${expectedExitCode}, exit=${String(exitCode)}, signal=${String(signalCode)})`,
        );
      }
      return Object.freeze({
        pid: requireChildPid(child),
        exitCode,
        signalCode,
      });
    } catch (error) {
      await terminateProcess(child).catch(() => undefined);
      throw new Error(
        this.redact(`secondary process failed: ${errorText(error)}\n\n${this.processDiagnostics(record)}`),
      );
    }
  }

  async createAdditionalRepository(name: string): Promise<string> {
    this.assertFixtureOpen("create an additional repository");
    if (!ADDITIONAL_REPOSITORY_NAME.test(name) || WINDOWS_RESERVED_NAME.test(name)) {
      throw new Error("additional repository name is invalid");
    }

    const repositoriesRoot = path.join(this.root, "additional-repositories");
    const repositoryDir = requireStrictFixturePath(
      this.root,
      path.join(repositoriesRoot, name),
      "additional repository",
    );
    await mkdir(repositoriesRoot, { recursive: true });
    await validateFixtureDirectory(
      this.root,
      repositoriesRoot,
      "additional repository root",
    );
    await assertPathAbsent(repositoryDir, "additional repository");

    try {
      await createRepository(repositoryDir);
      await validateFixtureDirectory(this.root, repositoryDir, "additional repository");
      return repositoryDir;
    } catch (error) {
      await rm(repositoryDir, { recursive: true, force: true }).catch(() => undefined);
      throw error;
    }
  }

  async repositorySnapshot(repositoryDir = this.repositoryDir): Promise<RepositorySnapshot> {
    const validatedRepositoryDir = await validateFixtureDirectory(
      this.root,
      repositoryDir,
      "snapshot repository",
    );
    const [files, statusResult, lockfile] = await Promise.all([
      execFileAsync("git", ["ls-files"], {
        cwd: validatedRepositoryDir,
        encoding: "utf8",
        windowsHide: true,
      }),
      execFileAsync("git", ["status", "--porcelain=v1", "--untracked-files=all"], {
        cwd: validatedRepositoryDir,
        encoding: "utf8",
        windowsHide: true,
      }),
      readFile(path.join(validatedRepositoryDir, "Cargo.lock"), "utf8"),
    ]);
    return {
      trackedFiles: files.stdout.split(/\r?\n/u).filter((entry) => entry.length > 0),
      lockfile,
      dirtyStatus: statusResult.stdout,
    };
  }

  async waitForCleanExit(timeoutMs = CLEAN_EXIT_TIMEOUT_MS): Promise<void> {
    await this.waitForExitCode(0, timeoutMs);
  }

  async waitForExitCode(
    expectedExitCode: number,
    timeoutMs = CLEAN_EXIT_TIMEOUT_MS,
  ): Promise<void> {
    if (!Number.isInteger(expectedExitCode)) {
      throw new Error("expected process exit code must be an integer");
    }
    const deadline = Date.now() + timeoutMs;
    const child = this.active.child;
    const exited = await waitForPromise(waitForExit(child), remainingMilliseconds(deadline));
    if (!exited) {
      throw new Error(
        this.redact(
          `coding-agent did not exit within ${timeoutMs} ms after the protected quit\n\n${this.diagnostics()}`,
        ),
      );
    }
    if (child.exitCode !== expectedExitCode || child.signalCode !== null) {
      throw new Error(
        this.redact(
          `coding-agent exit mismatch (expected=${String(expectedExitCode)}, exit=${String(child.exitCode)}, signal=${String(child.signalCode)})\n\n${this.diagnostics()}`,
        ),
      );
    }
    const descriptorRemoved = await waitForPathAbsent(
      this.descriptorPath,
      remainingMilliseconds(deadline),
    );
    if (!descriptorRemoved) {
      throw new Error(
        this.redact(`runtime descriptor remained after process exit\n\n${this.diagnostics()}`),
      );
    }
  }

  async attachDiagnostics(testInfo: TestInfo): Promise<void> {
    const diagnosticPath = testInfo.outputPath("coding-agent-process.log");
    await writeFile(diagnosticPath, this.redact(this.diagnostics()), {
      encoding: "utf8",
      mode: 0o600,
    });
    await testInfo.attach("coding-agent-process.log", {
      path: diagnosticPath,
      contentType: "text/plain",
    });
  }

  async stop(): Promise<void> {
    this.stopPromise ??= this.stopOnce();
    return this.stopPromise;
  }

  private async stopOnce(): Promise<void> {
    const failures: string[] = [];
    for (const process of this.processes) {
      await terminateProcess(process.child).catch((error: unknown) => {
        failures.push(`could not stop ${process.role} ${String(process.child.pid)}: ${errorText(error)}`);
      });
    }
    await rm(this.root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }).catch(
      (error: unknown) => failures.push(`could not remove isolated root: ${errorText(error)}`),
    );
    if (failures.length > 0) {
      throw new Error(this.redact(`${failures.join("\n")}\n\n${this.diagnostics()}`));
    }
  }

  private async navigateToGrant(page: Page, launchUrl: string): Promise<void> {
    try {
      await page.goto(launchUrl);
    } catch (error) {
      throw new Error(this.redact(`could not navigate to the local launch grant: ${errorText(error)}`));
    }
  }

  private rememberGrant(launchUrl: string): void {
    const token = launchToken(launchUrl);
    if (token.length > 0) {
      this.secrets.add(token);
    }
  }

  private diagnostics(): string {
    return this.processes.map((process) => this.processDiagnostics(process)).join("\n\n");
  }

  private processDiagnostics(process: ProcessRecord): string {
    return [
      `${process.role}: pid=${String(process.child.pid)} exit=${String(process.child.exitCode)} signal=${String(process.child.signalCode)}`,
      `stdout:\n${process.stdout.text()}`,
      `stderr:\n${process.stderr.text()}`,
    ].join("\n");
  }

  private redact(value: string): string {
    return redact(value, this.secrets);
  }

  private assertFixtureOpen(action: string): void {
    if (this.stopPromise !== null) {
      throw new Error(`cannot ${action} after isolated fixture cleanup started`);
    }
  }
}

export async function withLocalApp<T>(
  testInfo: TestInfo,
  scenario: ScenarioSource,
  run: (app: LocalApp) => Promise<T>,
): Promise<T> {
  const app = await startLocalApp(scenario);
  let result: T | undefined;
  let failure: unknown = null;
  try {
    result = await run(app);
  } catch (error) {
    failure = error;
    await app.attachDiagnostics(testInfo).catch(() => undefined);
  }

  try {
    await app.stop();
  } catch (cleanupError) {
    await app.attachDiagnostics(testInfo).catch(() => undefined);
    if (failure === null) {
      failure = cleanupError;
    } else {
      failure = new AggregateError(
        [failure, cleanupError],
        "the local-app scenario and its isolated cleanup both failed",
      );
    }
  }

  if (failure !== null) {
    throw failure;
  }
  return result as T;
}

export async function startLocalApp(
  scenarioSource: ScenarioSource = successScenario(),
): Promise<LocalApp> {
  const binary = process.env.CODING_AGENT_E2E_BINARY;
  if (!binary) {
    throw new Error("CODING_AGENT_E2E_BINARY must name the explicit e2e application binary");
  }
  const absoluteBinary = path.resolve(binary);
  await access(absoluteBinary, fsConstants.X_OK).catch(() => access(absoluteBinary));

  const root = await mkdtemp(path.join(os.tmpdir(), "ngy-coding-agent-e2e-"));
  const appDataDir = path.join(root, "app-data");
  const runtimeDir = path.join(root, "runtime");
  const signalDir = path.join(runtimeDir, "signals");
  const repositoryDir = path.join(root, "fixture-repository");
  const descriptorPath = path.join(runtimeDir, "instance.json");
  let child: AppProcess | null = null;
  let descriptor: RuntimeDescriptor | null = null;
  let launchUrl: string | null = null;
  const stdout = new BoundedLog();
  const stderr = new BoundedLog();

  try {
    await Promise.all([
      mkdir(appDataDir, { recursive: true }),
      mkdir(signalDir, { recursive: true }),
      createRepository(repositoryDir),
    ]);
    const roots = createScenarioRoots(root, appDataDir, runtimeDir, repositoryDir);
    const scenarioPath = await writeFreshScenario(roots, scenarioSource);

    child = spawnConfiguredProcess(absoluteBinary, root, appDataDir, runtimeDir, scenarioPath);
    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));

    descriptor = await waitForDescriptor(descriptorPath, child);
    await assertScenarioConsumed(scenarioPath);
    const grant = await requestReopenGrant(descriptor);
    launchUrl = grant.url;
    launchUrl = validateReopenGrant(grant, descriptor.port);
    return new LocalApp({
      root,
      appDataDir,
      runtimeDir,
      repositoryDir,
      descriptorPath,
      descriptor,
      launchUrl,
      child,
      stdout,
      stderr,
      absoluteBinary,
      roots,
    });
  } catch (error) {
    const cleanupErrors: string[] = [];
    if (child !== null) {
      await terminateProcess(child).catch((cleanupError: unknown) => {
        cleanupErrors.push(`process cleanup failed: ${errorText(cleanupError)}`);
      });
    }
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }).catch(
      (cleanupError: unknown) => {
        cleanupErrors.push(`temporary-root cleanup failed: ${errorText(cleanupError)}`);
      },
    );

    const secrets = new Set<string>();
    if (descriptor !== null) secrets.add(descriptor.launcher_secret);
    if (launchUrl !== null) secrets.add(launchToken(launchUrl));
    const cleanupSuffix = cleanupErrors.length === 0 ? "" : `\n${cleanupErrors.join("\n")}`;
    const diagnostics = [
      `coding-agent startup failed: ${errorText(error)}${cleanupSuffix}`,
      `stdout:\n${stdout.text()}`,
      `stderr:\n${stderr.text()}`,
    ].join("\n\n");
    throw new Error(redact(diagnostics, secrets));
  }
}

function createScenarioRoots(
  root: string,
  appDataDir: string,
  runtimeDir: string,
  repositoryDir: string,
): ScenarioRoots {
  const signalDir = path.join(runtimeDir, "signals");
  return Object.freeze({
    root,
    appDataDir,
    runtimeDir,
    repositoryDir,
    releaseSignalPath(name: string): string {
      if (!isValidReleaseSignalName(name)) {
        throw new Error(
          "virtual release signal name must be a non-reserved [A-Za-z0-9._-]{1,64} component",
        );
      }
      return path.join(signalDir, `${name}.release`);
    },
  });
}

async function writeFreshScenario(
  roots: ScenarioRoots,
  scenarioSource: ScenarioSource,
): Promise<string> {
  const scenario =
    typeof scenarioSource === "function" ? await scenarioSource(roots) : scenarioSource;
  await validateScenarioReleaseSignals(scenario, roots.runtimeDir);
  // Every process gets a distinct source path. Rust owns and consumes these
  // bytes once before it attempts either the primary or secondary path.
  const scenarioPath = path.join(roots.appDataDir, `scenario-${randomUUID()}.json`);
  await writeFile(scenarioPath, `${JSON.stringify(scenario)}\n`, {
    encoding: "utf8",
    mode: 0o600,
    flag: "wx",
  });
  return scenarioPath;
}

function spawnConfiguredProcess(
  absoluteBinary: string,
  root: string,
  appDataDir: string,
  runtimeDir: string,
  scenarioPath: string,
): AppProcess {
  return spawn(absoluteBinary, [], {
    cwd: root,
    env: {
      ...process.env,
      [TEST_APP_DATA_ENV]: appDataDir,
      [TEST_RUNTIME_ENV]: runtimeDir,
      [TEST_SCENARIO_ENV]: scenarioPath,
    },
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
}

async function spawnPrimaryGeneration(options: {
  absoluteBinary: string;
  roots: ScenarioRoots;
  descriptorPath: string;
  scenarioSource: ScenarioSource;
  previousInstanceId?: string;
}): Promise<PrimaryGeneration> {
  const scenarioPath = await writeFreshScenario(options.roots, options.scenarioSource);
  const stdout = new BoundedLog();
  const stderr = new BoundedLog();
  const child = spawnConfiguredProcess(
    options.absoluteBinary,
    options.roots.root,
    options.roots.appDataDir,
    options.roots.runtimeDir,
    scenarioPath,
  );
  child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
  child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
  let descriptor: RuntimeDescriptor | null = null;
  let launchUrl: string | null = null;
  try {
    descriptor = await waitForDescriptor(
      options.descriptorPath,
      child,
      options.previousInstanceId,
    );
    await assertScenarioConsumed(scenarioPath);
    const grant = await requestReopenGrant(descriptor);
    launchUrl = validateReopenGrant(grant, descriptor.port);
    return { child, descriptor, initialLaunchUrl: launchUrl, stdout, stderr };
  } catch (error) {
    await terminateProcess(child).catch(() => undefined);
    await rm(scenarioPath, { force: true }).catch(() => undefined);
    const secrets = new Set<string>();
    if (descriptor !== null) secrets.add(descriptor.launcher_secret);
    if (launchUrl !== null) secrets.add(launchToken(launchUrl));
    throw new Error(
      redact(
        [
          `primary generation startup failed: ${errorText(error)}`,
          `stdout:\n${stdout.text()}`,
          `stderr:\n${stderr.text()}`,
        ].join("\n\n"),
        secrets,
      ),
    );
  }
}

async function readRuntimeDescriptor(descriptorPath: string): Promise<RuntimeDescriptor> {
  const metadata = await lstat(descriptorPath);
  if (!metadata.isFile() || metadata.isSymbolicLink()) {
    throw new Error("runtime descriptor is not a regular file");
  }
  if (metadata.size <= 0 || metadata.size > MAX_DESCRIPTOR_BYTES) {
    throw new Error(`runtime descriptor size ${metadata.size} is invalid`);
  }
  return parseRuntimeDescriptor(JSON.parse(await readFile(descriptorPath, "utf8")) as unknown);
}

async function waitForScenarioConsumed(
  scenarioPath: string,
  child: AppProcess,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (true) {
    try {
      await lstat(scenarioPath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
      throw error;
    }
    if (!processIsRunning(child)) {
      throw new Error("process exited before consuming its fresh scenario");
    }
    const remaining = remainingMilliseconds(deadline);
    if (remaining === 0) {
      throw new Error(`process did not consume its fresh scenario within ${timeoutMs} ms`);
    }
    await delay(Math.min(10, remaining));
  }
}

export function deriveReachedSignalPath(runtimeDir: string, releasePath: string): string {
  const normalizedRelease = requireRuntimeFilePath(
    runtimeDir,
    releasePath,
    "virtual release signal",
  );
  return requireRuntimeFilePath(
    runtimeDir,
    `${normalizedRelease}.reached`,
    "actor reached marker",
  );
}

export async function waitForReachedSignal(
  runtimeDir: string,
  releasePath: string,
  timeoutMs = REACHED_SIGNAL_TIMEOUT_MS,
): Promise<ReachedSignal> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new Error("reached-marker timeout must be a positive finite number");
  }
  const normalizedRuntime = requireNormalizedAbsolutePath(runtimeDir, "runtime root");
  const normalizedRelease = requireRuntimeFilePath(
    normalizedRuntime,
    releasePath,
    "virtual release signal",
  );
  const reachedPath = deriveReachedSignalPath(normalizedRuntime, normalizedRelease);
  await assertSignalParent(normalizedRuntime, reachedPath);

  const deadline = Date.now() + timeoutMs;
  while (true) {
    try {
      await assertEmptyRegularFile(reachedPath, "actor reached marker");
      const reached: ReachedSignal = {
        runtimeDir: normalizedRuntime,
        releasePath: normalizedRelease,
        reachedPath,
        [reachedSignalBrand]: true,
      };
      return Object.freeze(reached);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    const remaining = remainingMilliseconds(deadline);
    if (remaining === 0) {
      throw new Error(`timed out after ${timeoutMs} ms waiting for actor reached marker`);
    }
    await delay(Math.min(10, remaining));
  }
}

export async function publishReleaseSignal(reached: ReachedSignal): Promise<void> {
  if (reached[reachedSignalBrand] !== true) {
    throw new Error("release publication requires a reached marker returned by the harness");
  }
  const runtimeDir = requireNormalizedAbsolutePath(reached.runtimeDir, "runtime root");
  const releasePath = requireRuntimeFilePath(
    runtimeDir,
    reached.releasePath,
    "virtual release signal",
  );
  const expectedReached = deriveReachedSignalPath(runtimeDir, releasePath);
  if (reached.reachedPath !== expectedReached) {
    throw new Error("reached marker does not match its virtual release signal");
  }
  await assertSignalParent(runtimeDir, releasePath);
  await assertEmptyRegularFile(expectedReached, "actor reached marker");
  await publishVirtualReleaseFile(runtimeDir, releasePath);
}

/**
 * Publishes a release for runner/store targets, which intentionally have no
 * actor reached marker. Actor targets must use `waitForReachedSignal` and
 * `publishReleaseSignal` instead.
 */
export async function publishUncoordinatedReleaseSignal(
  runtimeDir: string,
  releasePath: string,
): Promise<void> {
  const normalizedRuntime = requireNormalizedAbsolutePath(runtimeDir, "runtime root");
  const normalizedRelease = requireRuntimeFilePath(
    normalizedRuntime,
    releasePath,
    "virtual release signal",
  );
  await publishVirtualReleaseFile(normalizedRuntime, normalizedRelease);
}

async function publishVirtualReleaseFile(
  runtimeDir: string,
  releasePath: string,
): Promise<void> {
  await assertSignalParent(runtimeDir, releasePath);
  await assertPathAbsent(releasePath, "virtual release signal");

  const temporaryPath = path.join(
    path.dirname(releasePath),
    `.coding-agent-release-${randomUUID()}.tmp`,
  );
  let handle: Awaited<ReturnType<typeof open>> | null = null;
  try {
    handle = await open(temporaryPath, "wx", 0o600);
    await handle.sync();
    await handle.close();
    handle = null;
    // A same-directory hard link makes the already-synced empty file visible
    // atomically and fails rather than replacing an existing signal.
    await link(temporaryPath, releasePath);
  } finally {
    await handle?.close().catch(() => undefined);
    await rm(temporaryPath, { force: true }).catch(() => undefined);
  }
}

async function validateScenarioReleaseSignals(
  scenario: ProcessScenario,
  runtimeDir: string,
): Promise<void> {
  for (const signal of scenario.virtual_release_signals) {
    const releasePath = requireRuntimeFilePath(
      runtimeDir,
      signal.path,
      `virtual release signal ${signal.name}`,
    );
    await assertSignalParent(runtimeDir, releasePath);
  }
}

function requireNormalizedAbsolutePath(candidate: string, label: string): string {
  if (!path.isAbsolute(candidate) || path.normalize(candidate) !== candidate) {
    throw new Error(`${label} path must be normalized and absolute`);
  }
  return candidate;
}

function requireStrictFixturePath(fixtureRoot: string, candidate: string, label: string): string {
  const normalizedRoot = requireNormalizedAbsolutePath(fixtureRoot, "fixture root");
  const normalizedCandidate = requireNormalizedAbsolutePath(candidate, label);
  const relative = path.relative(normalizedRoot, normalizedCandidate);
  if (
    relative.length === 0 ||
    relative === ".." ||
    relative.startsWith(`..${path.sep}`) ||
    path.isAbsolute(relative)
  ) {
    throw new Error(`${label} path must be strictly inside the isolated fixture root`);
  }
  return normalizedCandidate;
}

async function validateFixtureDirectory(
  fixtureRoot: string,
  candidate: string,
  label: string,
): Promise<string> {
  const normalizedCandidate = requireStrictFixturePath(fixtureRoot, candidate, label);
  const [rootMetadata, candidateMetadata, canonicalRoot, canonicalCandidate] = await Promise.all([
    lstat(fixtureRoot),
    lstat(normalizedCandidate),
    realpath(fixtureRoot),
    realpath(normalizedCandidate),
  ]);
  if (
    !rootMetadata.isDirectory() ||
    rootMetadata.isSymbolicLink() ||
    !candidateMetadata.isDirectory() ||
    candidateMetadata.isSymbolicLink()
  ) {
    throw new Error(`${label} must be a regular directory`);
  }
  requireStrictFixturePath(canonicalRoot, canonicalCandidate, label);
  return normalizedCandidate;
}

function requireRuntimeFilePath(runtimeDir: string, candidate: string, label: string): string {
  const normalizedRuntime = requireNormalizedAbsolutePath(runtimeDir, "runtime root");
  const normalizedCandidate = requireNormalizedAbsolutePath(candidate, label);
  const signalDirectory = path.join(normalizedRuntime, "signals");
  if (path.dirname(normalizedCandidate) !== signalDirectory) {
    throw new Error(`${label} path must be a direct child of the dedicated signals directory`);
  }
  return normalizedCandidate;
}

async function assertSignalParent(runtimeDir: string, target: string): Promise<void> {
  const signalDirectory = path.join(runtimeDir, "signals");
  const [runtimeMetadata, signalMetadata, parentMetadata] = await Promise.all([
    lstat(runtimeDir),
    lstat(signalDirectory),
    lstat(path.dirname(target)),
  ]);
  if (!runtimeMetadata.isDirectory() || runtimeMetadata.isSymbolicLink()) {
    throw new Error("runtime root is not a regular directory");
  }
  if (
    !signalMetadata.isDirectory() ||
    signalMetadata.isSymbolicLink() ||
    !parentMetadata.isDirectory() ||
    parentMetadata.isSymbolicLink()
  ) {
    throw new Error("virtual signal parent is not a regular directory");
  }
  const [canonicalSignals, canonicalParent] = await Promise.all([
    realpath(signalDirectory),
    realpath(path.dirname(target)),
  ]);
  if (canonicalParent !== canonicalSignals) {
    throw new Error("virtual signal parent is not the dedicated signals directory");
  }
}

async function assertEmptyRegularFile(target: string, label: string): Promise<void> {
  const metadata = await lstat(target);
  if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.size !== 0) {
    throw new Error(`${label} must be an empty regular file`);
  }
}

async function assertPathAbsent(target: string, label: string): Promise<void> {
  try {
    await lstat(target);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw error;
  }
  throw new Error(`${label} already exists`);
}

async function createRepository(repositoryDir: string): Promise<void> {
  await mkdir(path.join(repositoryDir, "src"), { recursive: true });
  await writeFile(
    path.join(repositoryDir, "Cargo.toml"),
    '[package]\nname = "e2e-fixture"\nversion = "0.1.0"\nedition = "2024"\n',
    "utf8",
  );
  await writeFile(
    path.join(repositoryDir, "src", "lib.rs"),
    "pub fn fixture_value() -> u32 { 42 }\n",
    "utf8",
  );
  await execFileAsync("cargo", ["generate-lockfile"], { cwd: repositoryDir, windowsHide: true });
  await execFileAsync("git", ["init", "--initial-branch=main"], {
    cwd: repositoryDir,
    windowsHide: true,
  });
  await execFileAsync("git", ["add", "."], { cwd: repositoryDir, windowsHide: true });
  await execFileAsync(
    "git",
    [
      "-c",
      "user.name=NGY E2E",
      "-c",
      "user.email=e2e@example.invalid",
      "commit",
      "-m",
      "fixture",
    ],
    { cwd: repositoryDir, windowsHide: true },
  );
}

async function waitForDescriptor(
  descriptorPath: string,
  child: AppProcess,
  previousInstanceId?: string,
): Promise<RuntimeDescriptor> {
  const deadline = Date.now() + START_TIMEOUT_MS;
  let spawnError: Error | null = null;
  const onSpawnError = (error: Error) => {
    spawnError = error;
  };
  child.once("error", onSpawnError);
  try {
    while (Date.now() < deadline) {
      if (spawnError !== null) {
        throw spawnError;
      }
      if (child.exitCode !== null || child.signalCode !== null) {
        throw new Error(
          `coding-agent process exited before readiness (${String(child.exitCode ?? child.signalCode)})`,
        );
      }
      let descriptor: RuntimeDescriptor;
      try {
        descriptor = await readRuntimeDescriptor(descriptorPath);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") {
          await delay(25);
          continue;
        }
        throw error;
      }
      if (previousInstanceId === undefined) {
        if (child.pid === undefined || descriptor.pid !== child.pid) {
          throw new Error("runtime descriptor PID does not match the spawned process");
        }
        return descriptor;
      }
      // A hard-killed primary may leave a valid stale descriptor. The product
      // owns replacing it; the harness waits for the new atomic generation.
      if (descriptor.pid !== child.pid || descriptor.instance_id === previousInstanceId) {
        await delay(25);
        continue;
      }
      return descriptor;
    }
  } finally {
    child.off("error", onSpawnError);
  }
  throw new Error("timed out waiting for the atomic runtime descriptor");
}

function parseRuntimeDescriptor(candidate: unknown): RuntimeDescriptor {
  if (!isRecord(candidate)) {
    throw new Error("runtime descriptor is not an object");
  }
  const { instance_id, pid, port, started_at, launcher_secret } = candidate;
  if (
    typeof instance_id !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(
      instance_id,
    ) ||
    !Number.isInteger(pid) ||
    (pid as number) <= 0 ||
    !Number.isInteger(port) ||
    (port as number) <= 0 ||
    (port as number) > 65_535 ||
    typeof started_at !== "string" ||
    !UTC_TIMESTAMP.test(started_at) ||
    typeof launcher_secret !== "string" ||
    !isCanonicalSecret(launcher_secret) ||
    !hasExactKeys(candidate, ["instance_id", "launcher_secret", "pid", "port", "started_at"])
  ) {
    throw new Error("runtime descriptor fields are invalid");
  }
  return {
    instance_id,
    pid: pid as number,
    port: port as number,
    started_at,
    launcher_secret,
  };
}

async function assertScenarioConsumed(scenarioPath: string): Promise<void> {
  try {
    await stat(scenarioPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return;
    }
    throw error;
  }
  throw new Error("test scenario source bytes remain after process startup");
}

async function requestReopenGrant(descriptor: RuntimeDescriptor): Promise<ReopenGrant> {
  const candidate = await requestJson({
    port: descriptor.port,
    path: "/_local/reopen",
    method: "POST",
    launcherSecret: descriptor.launcher_secret,
  });
  if (
    !isRecord(candidate) ||
    typeof candidate.url !== "string" ||
    typeof candidate.expires_at !== "string" ||
    !UTC_TIMESTAMP.test(candidate.expires_at) ||
    !hasExactKeys(candidate, ["expires_at", "url"])
  ) {
    throw new Error("reopen returned an invalid response shape");
  }
  return { url: candidate.url, expires_at: candidate.expires_at };
}

function validateReopenGrant(grant: ReopenGrant, port: number): string {
  let parsed: URL;
  try {
    parsed = new URL(grant.url);
  } catch {
    throw new Error("reopen returned an invalid URL");
  }
  const expectedOrigin = `http://127.0.0.1:${port}`;
  const token = parsed.hash.startsWith("#token=")
    ? parsed.hash.slice("#token=".length)
    : "";
  const tokenCharsetOk = isCanonicalSecret(token);
  const canonicalEquality = grant.url === `${expectedOrigin}/#token=${token}`;
  const failures: string[] = [];
  if (parsed.origin !== expectedOrigin) failures.push("origin");
  if (parsed.protocol !== "http:") failures.push("protocol");
  if (parsed.hostname !== "127.0.0.1") failures.push("hostname");
  if (parsed.port !== String(port)) failures.push("port");
  if (parsed.username !== "" || parsed.password !== "") failures.push("credentials");
  if (parsed.pathname !== "/") failures.push("path");
  if (parsed.search !== "") failures.push("query");
  if (!tokenCharsetOk) failures.push("token_format");
  if (parsed.hash !== `#token=${token}`) failures.push("fragment");
  if (!canonicalEquality || parsed.href !== grant.url) failures.push("serialization");
  if (failures.length > 0) {
    throw new Error(
      [
        `reopen returned a non-canonical or unexpected launch URL (${failures.join(", ")})`,
        `actual_origin=${parsed.origin}`,
        `actual_path=${parsed.pathname}`,
        `search_length=${parsed.search.length}`,
        `token_length=${token.length}`,
        `token_base64url=${String(tokenCharsetOk)}`,
        `canonical_equality=${String(canonicalEquality)}`,
      ].join("; "),
    );
  }
  return grant.url;
}

async function requestJson(options: {
  port: number;
  path: string;
  method: "GET" | "POST";
  launcherSecret: string;
}): Promise<unknown> {
  return new Promise<unknown>((resolve, reject) => {
    let settled = false;
    const finish = (callback: () => void) => {
      if (settled) return;
      settled = true;
      callback();
    };
    const request = http.request(
      {
        hostname: "127.0.0.1",
        port: options.port,
        path: options.path,
        method: options.method,
        headers: {
          "x-launcher-secret": options.launcherSecret,
        },
        timeout: 5_000,
      },
      (response) => {
        const chunks: Buffer[] = [];
        let length = 0;
        response.on("data", (chunk: Buffer) => {
          length += chunk.length;
          if (length > MAX_HTTP_RESPONSE_BYTES) {
            response.destroy(new Error("local launcher response exceeded the size limit"));
            return;
          }
          chunks.push(Buffer.from(chunk));
        });
        response.on("error", (error) => finish(() => reject(error)));
        response.on("end", () => {
          if (response.statusCode !== 200) {
            finish(() => reject(new Error(`local launcher request failed with ${response.statusCode}`)));
            return;
          }
          try {
            const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;
            finish(() => resolve(body));
          } catch {
            finish(() => reject(new Error("local launcher response was not valid JSON")));
          }
        });
      },
    );
    request.on("timeout", () => request.destroy(new Error("local launcher request timed out")));
    request.on("error", (error) => finish(() => reject(error)));
    request.end();
  });
}

async function terminateProcess(child: AppProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  const exited = waitForExit(child);
  let gracefulError: unknown = null;
  try {
    child.kill();
  } catch (error) {
    gracefulError = error;
  }
  if (await waitForPromise(exited, GRACEFUL_STOP_TIMEOUT_MS)) {
    return;
  }
  let forcedError: unknown = null;
  if (child.exitCode === null && child.signalCode === null) {
    try {
      child.kill("SIGKILL");
    } catch (error) {
      forcedError = error;
    }
  }
  if (!(await waitForPromise(exited, FORCED_STOP_TIMEOUT_MS))) {
    const errors = [gracefulError, forcedError]
      .filter((error) => error !== null)
      .map(errorText)
      .join("; ");
    throw new Error(
      `process ${String(child.pid)} remained alive after graceful and forced stop deadlines${errors.length === 0 ? "" : ` (${errors})`}`,
    );
  }
}

async function hardKillProcess(child: AppProcess, timeoutMs: number): Promise<void> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new Error("hard-kill timeout must be a positive finite number");
  }
  if (!processIsRunning(child)) return;
  const exited = waitForExit(child);
  child.kill("SIGKILL");
  if (!(await waitForPromise(exited, timeoutMs))) {
    throw new Error(`process ${String(child.pid)} remained alive after the hard-kill deadline`);
  }
}

function processIsRunning(child: AppProcess): boolean {
  return child.exitCode === null && child.signalCode === null;
}

function requireChildPid(child: AppProcess): number {
  if (child.pid === undefined || !Number.isInteger(child.pid) || child.pid <= 0) {
    throw new Error("spawned process did not expose a valid PID");
  }
  return child.pid;
}

function waitForExit(child: AppProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return Promise.resolve();
  }
  return new Promise((resolve) => child.once("exit", () => resolve()));
}

function waitForPromise(promise: Promise<void>, timeoutMs: number): Promise<boolean> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(false), timeoutMs);
    void promise.then(
      () => {
        clearTimeout(timer);
        resolve(true);
      },
      () => {
        clearTimeout(timer);
        resolve(false);
      },
    );
  });
}

function remainingMilliseconds(deadline: number): number {
  return Math.max(0, deadline - Date.now());
}

async function waitForPathAbsent(target: string, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (true) {
    try {
      await lstat(target);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return true;
      throw error;
    }
    const remaining = remainingMilliseconds(deadline);
    if (remaining === 0) return false;
    await delay(Math.min(25, remaining));
  }
}

function launchToken(launchUrl: string): string {
  try {
    const hash = new URL(launchUrl).hash;
    return hash.startsWith("#token=") ? hash.slice("#token=".length) : "";
  } catch {
    return "";
  }
}

function redact(value: string, secrets: Iterable<string>): string {
  let output = value;
  for (const secret of secrets) {
    if (secret.length === 0) continue;
    output = output.split(secret).join("<redacted>");
    // A tail-bounded log can begin in the middle of a secret. Scrub that one
    // truncation boundary without broadly replacing harmless token fragments.
    for (let length = secret.length - 1; length > 0; length -= 1) {
      const boundaryFragment = `${LOG_TRUNCATION_MARKER}${secret.slice(-length)}`;
      if (output.includes(boundaryFragment)) {
        output = output.replaceAll(boundaryFragment, `${LOG_TRUNCATION_MARKER}<redacted>`);
        break;
      }
    }
  }
  return output
    .replace(/#token=[^\s"']+/gu, "#token=<redacted>")
    .replace(/("launcher_secret"\s*:\s*")[^"]+("?)/gu, "$1<redacted>$2");
}

function errorText(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, expected: string[]): boolean {
  const actual = Object.keys(value).sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function isCanonicalSecret(value: string): boolean {
  if (!/^[A-Za-z0-9_-]{43}$/u.test(value)) return false;
  const decoded = Buffer.from(value, "base64url");
  return decoded.length === 32 && decoded.toString("base64url") === value;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

class BoundedLog {
  private bytes = Buffer.alloc(0);
  private truncated = false;

  push(chunk: Buffer): void {
    const combined = Buffer.concat([this.bytes, chunk]);
    if (combined.length > MAX_LOG_BYTES) {
      this.bytes = combined.subarray(combined.length - MAX_LOG_BYTES);
      this.truncated = true;
    } else {
      this.bytes = combined;
    }
  }

  text(): string {
    const prefix = this.truncated ? LOG_TRUNCATION_MARKER : "";
    return `${prefix}${this.bytes.toString("utf8")}`;
  }
}
