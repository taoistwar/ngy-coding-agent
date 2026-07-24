import {
  expect,
  test,
  type APIRequestContext,
  type Page,
  type Response,
} from "./fixtures";

import {
  publishUncoordinatedReleaseSignal,
  successScenario,
  withLocalApp,
  type LocalApp,
  type ProcessScenario,
} from "./support/localApp";

const API_TIMEOUT_MS = 10_000;

interface RepositoryView {
  id: string;
}

interface TaskView {
  id: string;
  prompt: string;
  status: string;
  failure: { code: string } | null;
}

interface EventView {
  id: number;
  kind: string;
}

const emptyScenario = (): ProcessScenario => ({
  ...successScenario(),
  fake_scenarios: [],
});

test("hard-kill restart interrupts every queued and running task without auto-resume", async (
  { context, page },
  testInfo,
) => {
  await withLocalApp(
    testInfo,
    {
      ...successScenario(),
      fake_scenarios: ["blocking", "blocking", "blocking", "blocking"],
    },
    async (app) => {
      await openApp(page, app);
      const repository = await addRepository(page, app);
      const prompts = Array.from({ length: 5 }, (_, index) => `Restart recovery task ${index + 1}`);
      const created: TaskView[] = [];
      for (const prompt of prompts) created.push(await createTask(page, app, prompt));

      const beforeKill = await waitForTasks(
        context.request,
        app.origin,
        (tasks) =>
          tasks.length === 5 &&
          tasks.filter((task) => task.status === "running").length === 4 &&
          tasks.filter((task) => task.status === "queued").length === 1,
        "four Running tasks and one Queued task",
      );
      expect(new Set(beforeKill.map((task) => task.id))).toEqual(
        new Set(created.map((task) => task.id)),
      );
      const taskList = page.getByRole("list", { name: "Tasks" });
      await expect(taskList.locator("li").filter({ hasText: "Running" })).toHaveCount(4);
      await expect(taskList.locator("li").filter({ hasText: "Queued" })).toHaveCount(1);

      const oldIdentity = await app.runtimeIdentity();
      await app.hardKillPrimaryPreservingRoot();
      await app.restart(emptyScenario());
      const newIdentity = await app.runtimeIdentity();
      expect(newIdentity.instanceId).not.toBe(oldIdentity.instanceId);
      await openApp(page, app);

      const recovered = await waitForTasks(
        context.request,
        app.origin,
        (tasks) => tasks.length === 5 && tasks.every((task) => task.status === "interrupted"),
        "five Interrupted tasks after restart recovery",
      );
      expect(recovered.every((task) => task.failure?.code === "APP_RESTARTED")).toBe(true);
      expect(recovered.every((task) => task.status !== "queued" && task.status !== "running")).toBe(
        true,
      );
      expect(recovered.every((task) => task.id.length > 0)).toBe(true);
      expect(repository.id).not.toBe("");
      await expect(taskList.locator("li").filter({ hasText: "Interrupted" })).toHaveCount(5);

      await page.waitForTimeout(1_200);
      const stable = await listTasks(context.request, app.origin);
      expect(stable.every((task) => task.status === "interrupted")).toBe(true);
      await quitThroughUi(page, app);
    },
  );
});

test("a terminal commit survives a kill before its writer wake", async (
  { context, page },
  testInfo,
) => {
  let runnerReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots) => {
      runnerReleasePath = roots.releaseSignalPath("complete-blocking-runner");
      return {
        ...successScenario(),
        fake_scenarios: ["blocking"],
        store_writer_faults: [
          {
            point: "pause_after_commit_before_wake",
            operation: "finalize_reviewed_task",
            count: 1,
          },
        ],
        virtual_release_signals: [
          {
            name: "complete-blocking-runner",
            path: runnerReleasePath,
            target: "runner_next",
          },
          {
            name: "finish-commit-before-wake",
            path: roots.releaseSignalPath("finish-commit-before-wake"),
            target: "store_writer_after_commit_before_wake",
          },
        ],
      };
    },
    async (app) => {
      await openApp(page, app);
      await addRepository(page, app);
      const prompt = "Commit before writer wake";
      const created = await createTask(page, app, prompt);
      await waitForTask(
        context.request,
        app.origin,
        created.id,
        (task) => task.status === "running",
        "blocking task to enter Running",
      );
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: running");
      if (runnerReleasePath.length === 0) throw new Error("runner release path was not configured");
      await publishUncoordinatedReleaseSignal(app.runtimeDir, runnerReleasePath);

      const durable = await waitForTask(
        context.request,
        app.origin,
        created.id,
        (task) => task.status === "completed",
        "durable Completed state before writer wake",
        15,
      );
      expect(durable.failure).toBeNull();
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: running");

      await app.hardKillPrimaryPreservingRoot();
      await app.restart(emptyScenario());
      await openApp(page, app);

      const restarted = await readTask(context.request, app.origin, created.id);
      expect(restarted.status).toBe("completed");
      expect(restarted.failure).toBeNull();
      const events = await taskEvents(context.request, app.origin, created.id);
      expect(events.filter((event) => event.kind === "task.completed")).toHaveLength(1);
      expect(events.filter((event) => event.kind === "task.interrupted")).toHaveLength(0);

      await page
        .getByRole("button", { name: `${prompt} Attempt 1`, exact: true })
        .click();
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: completed");
      await quitThroughUi(page, app);
    },
  );
});

test("a secondary exits without replacing the primary writer or descriptor", async (
  { context, page },
  testInfo,
) => {
  await withLocalApp(testInfo, successScenario(), async (app) => {
    await openApp(page, app);
    const beforeIdentity = await app.runtimeIdentity();
    const beforeCursor = await bootstrapCursor(context.request, app.origin);

    const secondaryStartedAt = Date.now();
    const secondary = await app.startSecondaryAndWait(emptyScenario(), 0, 10_000);
    expect(Date.now() - secondaryStartedAt).toBeLessThan(10_000);
    expect(secondary.exitCode).toBe(0);
    expect(secondary.signalCode).toBeNull();
    expect(secondary.pid).not.toBe(beforeIdentity.pid);

    const afterIdentity = await app.runtimeIdentity();
    expect(afterIdentity).toEqual(beforeIdentity);
    expect(await bootstrapCursor(context.request, app.origin)).toBe(beforeCursor);

    await addRepository(page, app);
    const task = await createTask(page, app, "Primary remains writable after secondary");
    await waitForTask(
      context.request,
      app.origin,
      task.id,
      (candidate) => candidate.status === "completed",
      "primary task completion after secondary exit",
    );
    await expect(page.locator(".task-status-label")).toHaveText("Execution status: completed");
    await quitThroughUi(page, app);
  });
});

async function openApp(page: Page, app: LocalApp): Promise<void> {
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
  return repositoryFromResponse(await response);
}

async function createTask(page: Page, app: LocalApp, prompt: string): Promise<TaskView> {
  const input = page.getByLabel("Task description");
  await expect(input).toHaveValue("");
  await input.fill(prompt);
  const response = page.waitForResponse(
    (candidate) =>
      candidate.request().method() === "POST" && candidate.url() === `${app.origin}/api/tasks`,
  );
  await page.getByRole("button", { name: "Create task" }).click();
  return taskFromResponse(await response);
}

async function repositoryFromResponse(response: Response): Promise<RepositoryView> {
  expect([200, 201]).toContain(response.status());
  const candidate = (await response.json()) as unknown;
  if (!isRecord(candidate) || typeof candidate.id !== "string") {
    throw new Error("repository response did not contain an ID");
  }
  return { id: candidate.id };
}

async function taskFromResponse(response: Response): Promise<TaskView> {
  expect([200, 201]).toContain(response.status());
  return parseTask(await response.json());
}

async function listTasks(api: APIRequestContext, origin: string): Promise<TaskView[]> {
  const response = await api.get(`${origin}/api/tasks`);
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!Array.isArray(candidate)) throw new Error("task list response was not an array");
  return candidate.map(parseTask);
}

async function readTask(
  api: APIRequestContext,
  origin: string,
  taskId: string,
): Promise<TaskView> {
  const response = await api.get(`${origin}/api/tasks/${taskId}`);
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!isRecord(candidate)) throw new Error("task detail response was not an object");
  return parseTask(candidate.task);
}

async function taskEvents(
  api: APIRequestContext,
  origin: string,
  taskId: string,
): Promise<EventView[]> {
  const response = await api.get(`${origin}/api/tasks/${taskId}/events?after=0`);
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!Array.isArray(candidate)) throw new Error("task events response was not an array");
  return candidate.map((event) => {
    if (!isRecord(event) || !Number.isInteger(event.id) || typeof event.kind !== "string") {
      throw new Error("task event response contained an invalid entry");
    }
    return { id: event.id as number, kind: event.kind };
  });
}

async function bootstrapCursor(api: APIRequestContext, origin: string): Promise<number> {
  const response = await api.get(`${origin}/api/bootstrap`);
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!isRecord(candidate) || !Number.isInteger(candidate.latest_event_id)) {
    throw new Error("bootstrap response did not contain an event cursor");
  }
  return candidate.latest_event_id as number;
}

async function waitForTasks(
  api: APIRequestContext,
  origin: string,
  predicate: (tasks: TaskView[]) => boolean,
  label: string,
): Promise<TaskView[]> {
  return pollUntil(() => listTasks(api, origin), predicate, label);
}

async function waitForTask(
  api: APIRequestContext,
  origin: string,
  taskId: string,
  predicate: (task: TaskView) => boolean,
  label: string,
  intervalMs = 25,
): Promise<TaskView> {
  return pollUntil(() => readTask(api, origin, taskId), predicate, label, intervalMs);
}

async function pollUntil<T>(
  load: () => Promise<T>,
  predicate: (value: T) => boolean,
  label: string,
  intervalMs = 25,
): Promise<T> {
  const deadline = Date.now() + API_TIMEOUT_MS;
  while (true) {
    const value = await load();
    if (predicate(value)) return value;
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error(`timed out waiting for ${label}`);
    await new Promise((resolve) => setTimeout(resolve, Math.min(intervalMs, remaining)));
  }
}

function parseTask(candidate: unknown): TaskView {
  if (
    !isRecord(candidate) ||
    typeof candidate.id !== "string" ||
    typeof candidate.prompt !== "string" ||
    typeof candidate.status !== "string"
  ) {
    throw new Error("task response contained invalid identity or status fields");
  }
  let failure: TaskView["failure"] = null;
  if (candidate.failure !== null && candidate.failure !== undefined) {
    if (!isRecord(candidate.failure) || typeof candidate.failure.code !== "string") {
      throw new Error("task response contained an invalid failure");
    }
    failure = { code: candidate.failure.code };
  }
  return {
    id: candidate.id,
    prompt: candidate.prompt,
    status: candidate.status,
    failure,
  };
}

async function quitThroughUi(page: Page, app: LocalApp): Promise<void> {
  await page.getByRole("button", { name: "Quit local application" }).click();
  const dialog = page.getByRole("dialog", { name: "Quit local application?" });
  await expect(dialog).toBeVisible();
  await Promise.all([
    app.waitForCleanExit(),
    dialog.getByRole("button", { name: "Quit application" }).click(),
  ]);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
