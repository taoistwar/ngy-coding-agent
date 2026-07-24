import { lstat, readFile } from "node:fs/promises";
import path from "node:path";

import { expect, test, type APIRequestContext, type Page, type Response } from "./fixtures";

import {
  publishReleaseSignal,
  waitForReachedSignal,
  withLocalApp,
  type LocalApp,
  type ProcessScenario,
  type ReachedSignal,
  type ScenarioRoots,
} from "./support/localApp";

const CLEAN_EXIT_BUDGET_MS = 10_000;
const STARTUP_PROBE_BUDGET_MS = 10_000;
const BROWSER_PROBE_FILE = "browser-invoked.probe";
const RECOVERY_PROBE_FILE = "startup-recovery.json";

interface RepositoryView {
  id: string;
}

interface TaskView {
  id: string;
  prompt: string;
  status: string;
  failure: { code: string } | null;
}

test("descriptor publication precedes browser dispatch with positive and negative evidence", async (
  { page },
  testInfo,
) => {
  let boundaryProbe: Promise<void> | null = null;
  let runError: unknown = null;

  try {
    await withLocalApp(
      testInfo,
      (roots): ProcessScenario => {
        const releasePath = roots.releaseSignalPath("descriptor-before-browser");
        boundaryProbe = probeBrowserBoundary(roots, releasePath);
        void boundaryProbe.catch(() => undefined);
        return {
          fake_scenarios: [],
          store_writer_faults: [],
          actor_pauses: ["descriptor_before_browser"],
          virtual_release_signals: [
            {
              name: "descriptor-before-browser",
              path: releasePath,
              target: "actor_descriptor_before_browser",
            },
          ],
          legacy_v2_seed: { kind: "none" },
          marker_write_failure: false,
        };
      },
      async (app) => {
        await requireBoundaryProbe(boundaryProbe);
        await waitForRegularFile(
          path.join(app.runtimeDir, "signals", BROWSER_PROBE_FILE),
          "browser dispatch probe",
        );
        await openWorkspace(page, app);
        await quitThroughUi(page, app);
      },
    );
  } catch (error) {
    runError = error;
  }

  let probeError: unknown = null;
  try {
    await requireBoundaryProbe(boundaryProbe);
  } catch (error) {
    probeError = error;
  }

  throwCombined(runError, probeError, "browser-boundary scenario and detached probe both failed");
});

test("restart recovery completes before a replacement descriptor is published", async (
  { context, page },
  testInfo,
) => {
  await withLocalApp(
    testInfo,
    {
      fake_scenarios: ["blocking"],
      store_writer_faults: [],
      actor_pauses: [],
      virtual_release_signals: [],
      legacy_v2_seed: { kind: "none" },
      marker_write_failure: false,
    },
    async (app) => {
      await openWorkspace(page, app);
      const repository = await addRepository(page, app);
      expect(repository.id).not.toBe("");
      const created = await createTask(page, app, "Recovery ordering task");
      await waitForTaskStatus(
        context.request,
        app.origin,
        created.id,
        "running",
        "the recovery fixture task to enter Running",
      );
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: running");

      await app.hardKillPrimaryPreservingRoot();
      const releasePath = path.join(
        app.runtimeDir,
        "signals",
        "recovery-before-descriptor.release",
      );
      const restart = app.restart({
        fake_scenarios: [],
        store_writer_faults: [],
        actor_pauses: ["recovery_before_descriptor"],
        virtual_release_signals: [
          {
            name: "recovery-before-descriptor",
            path: releasePath,
            target: "actor_recovery_before_descriptor",
          },
        ],
        legacy_v2_seed: { kind: "none" },
        marker_write_failure: false,
      });
      void restart.catch(() => undefined);

      const reached = await waitForReachedSignal(app.runtimeDir, releasePath);
      let boundaryError: unknown = null;
      try {
        await assertPathAbsent(app.descriptorPath);
        const probe = await readRecoveryProbe(
          path.join(app.runtimeDir, "signals", RECOVERY_PROBE_FILE),
        );
        expect(probe.interrupted_count).toBe(1);
      } catch (error) {
        boundaryError = error;
      }

      let releaseError: unknown = null;
      try {
        await publishReleaseSignal(reached);
      } catch (error) {
        releaseError = error;
      }

      let restartError: unknown = null;
      try {
        await restart;
      } catch (error) {
        restartError = error;
      }
      throwMany(
        [boundaryError, releaseError, restartError],
        "recovery-boundary assertion, release, or restart failed",
      );

      await openWorkspace(page, app);
      const recovered = await waitForTaskStatus(
        context.request,
        app.origin,
        created.id,
        "interrupted",
        "the recovered task to become Interrupted",
      );
      expect(recovered.failure?.code).toBe("APP_RESTARTED");
      await page
        .getByRole("button", { name: "Recovery ordering task Attempt 1", exact: true })
        .click();
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: interrupted");
      await expect(page.getByRole("list", { name: "Tasks" })).toContainText("Interrupted");
      await quitThroughUi(page, app);
    },
  );
});

async function probeBrowserBoundary(
  roots: ScenarioRoots,
  releasePath: string,
): Promise<void> {
  const reached = await waitForReachedSignal(roots.runtimeDir, releasePath);
  const errors: unknown[] = [];

  try {
    await assertRegularFile(path.join(roots.runtimeDir, "instance.json"));
    await assertPathAbsent(path.join(roots.runtimeDir, "signals", BROWSER_PROBE_FILE));
  } catch (error) {
    errors.push(error);
  }

  try {
    await publishReleaseSignal(reached);
  } catch (error) {
    errors.push(error);
  }

  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) {
    throw new AggregateError(errors, "browser-boundary assertion and release both failed");
  }
}

async function readRecoveryProbe(target: string): Promise<{ interrupted_count: number }> {
  await assertRegularFile(target);
  const bytes = await readFile(target);
  try {
    const candidate = JSON.parse(bytes.toString("utf8")) as unknown;
    if (
      !isRecord(candidate) ||
      Object.keys(candidate).length !== 1 ||
      !Number.isInteger(candidate.interrupted_count) ||
      (candidate.interrupted_count as number) < 1
    ) {
      throw new Error("startup recovery probe did not contain a positive interrupted_count");
    }
    return { interrupted_count: candidate.interrupted_count as number };
  } finally {
    bytes.fill(0);
  }
}

async function assertPathAbsent(target: string): Promise<void> {
  try {
    await lstat(target);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw error;
  }
  throw new Error(`${path.basename(target)} was published before its ordering boundary`);
}

async function assertRegularFile(target: string): Promise<void> {
  const metadata = await lstat(target);
  if (!metadata.isFile() || metadata.isSymbolicLink()) {
    throw new Error(`${path.basename(target)} is not a regular file`);
  }
}

async function waitForRegularFile(target: string, label: string): Promise<void> {
  const deadline = Date.now() + STARTUP_PROBE_BUDGET_MS;
  while (Date.now() < deadline) {
    try {
      await assertRegularFile(target);
      return;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`${label} was not published within ${STARTUP_PROBE_BUDGET_MS} ms`);
}

async function requireBoundaryProbe(probe: Promise<void> | null): Promise<void> {
  if (probe === null) {
    throw new Error("scenario factory did not install the detached startup probe");
  }
  await probe;
}

function throwCombined(primary: unknown, secondary: unknown, message: string): void {
  if (primary !== null && secondary !== null && primary !== secondary) {
    throw new AggregateError([primary, secondary], message);
  }
  if (primary !== null) throw primary;
  if (secondary !== null) throw secondary;
}

function throwMany(errors: unknown[], message: string): void {
  const present = errors.filter((error) => error !== null);
  if (present.length === 1) throw present[0];
  if (present.length > 1) throw new AggregateError(present, message);
}

async function openWorkspace(page: Page, app: LocalApp): Promise<void> {
  const bootstrap = page.waitForResponse(
    (response) => response.url() === `${app.origin}/api/bootstrap` && response.status() === 200,
  );
  await app.open(page);
  await bootstrap;
  await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();
  await expect(page.getByRole("status")).toContainText("Connected");
}

async function addRepository(page: Page, app: LocalApp): Promise<RepositoryView> {
  await page.getByLabel("Repository path").fill(app.repositoryDir);
  const response = page.waitForResponse(
    (candidate) =>
      candidate.request().method() === "POST" &&
      candidate.url() === `${app.origin}/api/repositories`,
  );
  await page.getByRole("button", { name: "Add repository path" }).click();
  const candidate = await jsonFromResponse(await response, [200, 201]);
  if (!isRecord(candidate) || typeof candidate.id !== "string") {
    throw new Error("repository response did not contain an ID");
  }
  return { id: candidate.id };
}

async function createTask(page: Page, app: LocalApp, prompt: string): Promise<TaskView> {
  await page.getByLabel("Task description").fill(prompt);
  const response = page.waitForResponse(
    (candidate) =>
      candidate.request().method() === "POST" && candidate.url() === `${app.origin}/api/tasks`,
  );
  await page.getByRole("button", { name: "Create task", exact: true }).click();
  return parseTask(await jsonFromResponse(await response, [200, 201]));
}

async function waitForTaskStatus(
  api: APIRequestContext,
  origin: string,
  taskId: string,
  status: string,
  label: string,
): Promise<TaskView> {
  await expect
    .poll(
      async () => {
        const response = await api.get(`${origin}/api/tasks/${taskId}`);
        if (response.status() !== 200) return null;
        const candidate = (await response.json()) as unknown;
        if (!isRecord(candidate)) return null;
        const task = parseTask(candidate.task);
        return task.status === status ? task : null;
      },
      { message: `timed out waiting for ${label}`, timeout: STARTUP_PROBE_BUDGET_MS },
    )
    .not.toBeNull();

  const response = await api.get(`${origin}/api/tasks/${taskId}`);
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!isRecord(candidate)) throw new Error("task detail response was not an object");
  return parseTask(candidate.task);
}

async function jsonFromResponse(response: Response, expected: number[]): Promise<unknown> {
  expect(expected).toContain(response.status());
  return response.json() as Promise<unknown>;
}

function parseTask(candidate: unknown): TaskView {
  if (
    !isRecord(candidate) ||
    typeof candidate.id !== "string" ||
    typeof candidate.prompt !== "string" ||
    typeof candidate.status !== "string"
  ) {
    throw new Error("task response was invalid");
  }
  const failure = candidate.failure;
  return {
    id: candidate.id,
    prompt: candidate.prompt,
    status: candidate.status,
    failure:
      isRecord(failure) && typeof failure.code === "string" ? { code: failure.code } : null,
  };
}

async function quitThroughUi(page: Page, app: LocalApp): Promise<void> {
  await page.getByRole("button", { name: "Quit local application" }).click();
  const dialog = page.getByRole("dialog", { name: "Quit local application?" });
  await expect(dialog).toBeVisible();
  await Promise.all([
    app.waitForCleanExit(CLEAN_EXIT_BUDGET_MS),
    dialog.getByRole("button", { name: "Quit application" }).click(),
  ]);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
