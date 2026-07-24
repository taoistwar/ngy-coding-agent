import { expect, test, type Locator, type Page } from "./fixtures";

import {
  publishUncoordinatedReleaseSignal,
  withLocalApp,
  type LocalApp,
  type ProcessScenario,
} from "./support/localApp";

type TaskStatus =
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "interrupted";

interface TaskWire {
  id: string;
  prompt: string;
  status: TaskStatus;
  attempt: number;
  retry_of?: string | null;
  failure?: { code: string; message: string; retryable: boolean } | null;
}

interface TaskEventWire {
  kind: string;
}

const CONCURRENCY_PROMPTS = [
  "Blocking workflow 1",
  "Blocking workflow 2",
  "Blocking workflow 3",
  "Blocking workflow 4",
  "Blocking workflow 5",
  "Blocking workflow 6",
] as const;
const RETRY_PROMPT = "Failure panic success retry chain";

test("keeps four tasks running, cancels the queued fifth, and starts only the sixth", async ({
  page,
}, testInfo) => {
  let runnerReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      runnerReleasePath = roots.releaseSignalPath("release-first-runner");
      return {
        // Only five tasks can start: the first four and, after task five is
        // cancelled, task six. Every possible launch has an explicit script.
        fake_scenarios: ["blocking", "blocking", "blocking", "blocking", "blocking"],
        store_writer_faults: [],
        actor_pauses: [],
        virtual_release_signals: [
          {
            name: "release-first-runner",
            path: runnerReleasePath,
            target: "runner_next",
          },
        ],
        legacy_v2_seed: { kind: "none" },
        marker_write_failure: false,
      };
    },
    async (app) => {
      await openWorkspace(page, app);
      await addRepositoryThroughUi(page, app);

      const tasks: TaskWire[] = [];
      for (const prompt of CONCURRENCY_PROMPTS) {
        tasks.push(await createTaskThroughUi(page, app, prompt));
      }
      if (tasks.length !== 6) throw new Error("six task responses were expected");
      const [first, second, third, fourth, fifth, sixth] = tasks as [
        TaskWire,
        TaskWire,
        TaskWire,
        TaskWire,
        TaskWire,
        TaskWire,
      ];

      await expectTaskStatuses(page, {
        [first.id]: "running",
        [second.id]: "running",
        [third.id]: "running",
        [fourth.id]: "running",
        [fifth.id]: "queued",
        [sixth.id]: "queued",
      });
      for (const [index, prompt] of CONCURRENCY_PROMPTS.entries()) {
        await expect(taskItem(page, prompt).locator(".task-list-status"))
          .toContainText(index < 4 ? "Running" : "Queued");
      }

      await selectTask(page, CONCURRENCY_PROMPTS[4]);
      await expectSelectedStatus(page, "queued");
      await page.getByRole("button", { name: "Cancel task", exact: true }).click();
      await expectSelectedStatus(page, "cancelled");
      await expect(taskItem(page, CONCURRENCY_PROMPTS[4]).locator(".task-list-status"))
        .toContainText("Cancelled");
      expect(await taskEventKinds(page, fifth.id)).toEqual([
        "task.queued",
        "task.cancelled",
      ]);

      if (runnerReleasePath.length === 0) {
        throw new Error("runner release path was not initialized by the scenario factory");
      }
      await publishUncoordinatedReleaseSignal(app.runtimeDir, runnerReleasePath);
      await expectTaskStatuses(page, {
        [first.id]: "completed",
        [second.id]: "running",
        [third.id]: "running",
        [fourth.id]: "running",
        [fifth.id]: "cancelled",
        [sixth.id]: "running",
      });

      await selectTask(page, CONCURRENCY_PROMPTS[5]);
      await expectSelectedStatus(page, "running");
      await page.getByRole("button", { name: "Cancel task", exact: true }).click();
      await expectSelectedStatus(page, "cancelled");
      expect(await taskEventKinds(page, sixth.id)).toEqual([
        "task.queued",
        "task.started",
        "task.cancelled",
      ]);

      await expectTaskStatuses(page, {
        [first.id]: "completed",
        [second.id]: "running",
        [third.id]: "running",
        [fourth.id]: "running",
        [fifth.id]: "cancelled",
        [sixth.id]: "cancelled",
      });
      await assertProcessIsLive(page);
      await quitThroughUi(page, app);
    },
  );
});

test("isolates failure and panic across retries and keeps old attempts read-only", async ({
  page,
}, testInfo) => {
  const scenario: ProcessScenario = {
    fake_scenarios: ["failure", "panic", "success"],
    store_writer_faults: [],
    actor_pauses: [],
    virtual_release_signals: [],
    legacy_v2_seed: { kind: "none" },
    marker_write_failure: false,
  };

  await withLocalApp(testInfo, scenario, async (app) => {
    await openWorkspace(page, app);
    await addRepositoryThroughUi(page, app);

    const first = await createTaskThroughUi(page, app, RETRY_PROMPT);
    await expectSelectedStatus(page, "failed");
    await expectFailureCode(page, "FAKE_RUNNER_FAILURE");

    const second = await retrySelectedTaskThroughUi(page, app, first.id);
    await expectSelectedStatus(page, "failed");
    await expectFailureCode(page, "RUNNER_PANICKED");

    const third = await retrySelectedTaskThroughUi(page, app, second.id);
    await expectSelectedStatus(page, "completed");
    await expect(
      page.getByText("Delivery readiness: review approved", { exact: true }),
    ).toBeVisible();

    await expect(page.getByRole("button", { name: /Attempt 1.*failed/iu })).toBeVisible();
    await expect(page.getByRole("button", { name: /Attempt 2.*failed/iu })).toBeVisible();
    await expect(page.getByRole("button", { name: /Attempt 3.*completed/iu })).toBeVisible();

    await page.getByRole("button", { name: /Attempt 2.*failed/iu }).click();
    await expectSelectedStatus(page, "failed");
    await expectFailureCode(page, "RUNNER_PANICKED");
    await expect(page.getByText("Read-only attempt", { exact: true })).toBeVisible();
    await expect(
      page.locator(".task-actions").getByRole("button", { name: "Retry task", exact: true }),
    ).toHaveCount(0);

    await page.getByRole("button", { name: /Attempt 1.*failed/iu }).click();
    await expectSelectedStatus(page, "failed");
    await expectFailureCode(page, "FAKE_RUNNER_FAILURE");
    await expect(page.getByText("Read-only attempt", { exact: true })).toBeVisible();
    await expect(
      page.locator(".task-actions").getByRole("button", { name: "Retry task", exact: true }),
    ).toHaveCount(0);

    const persisted = await listTasks(page);
    expect(persisted).toHaveLength(3);
    const orderedAttempts = [...persisted].sort((left, right) => left.attempt - right.attempt);
    const [persistedFirst, persistedSecond, persistedThird] = orderedAttempts;
    if (
      persistedFirst === undefined ||
      persistedSecond === undefined ||
      persistedThird === undefined
    ) {
      throw new Error("three persisted retry attempts were expected");
    }
    expect(orderedAttempts.map(({ id, attempt, retry_of: retryOf, status }) => ({
      id,
      attempt,
      retryOf: retryOf ?? null,
      status,
    }))).toEqual([
      { id: first.id, attempt: 1, retryOf: null, status: "failed" },
      { id: second.id, attempt: 2, retryOf: first.id, status: "failed" },
      { id: third.id, attempt: 3, retryOf: second.id, status: "completed" },
    ]);
    expect(persistedFirst.failure?.code).toBe("FAKE_RUNNER_FAILURE");
    expect(persistedSecond.failure?.code).toBe("RUNNER_PANICKED");
    expect(persistedThird.failure ?? null).toBeNull();

    await assertProcessIsLive(page);
    await quitThroughUi(page, app);
  });
});

async function openWorkspace(page: Page, app: LocalApp): Promise<void> {
  await app.open(page);
  await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();
  await expect(page.locator(".connection-banner")).toContainText("Connected");
}

async function addRepositoryThroughUi(page: Page, app: LocalApp): Promise<void> {
  await page.getByLabel("Repository path").fill(app.repositoryDir);
  const responsePromise = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      response.url() === `${app.origin}/api/repositories`,
  );
  await page.getByRole("button", { name: "Add repository path" }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(201);
  await expect(page.getByRole("button", { name: /^fixture-repository /u })).toBeVisible();
}

async function createTaskThroughUi(
  page: Page,
  app: LocalApp,
  prompt: string,
): Promise<TaskWire> {
  await page.getByLabel("Task description").fill(prompt);
  const responsePromise = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" && response.url() === `${app.origin}/api/tasks`,
  );
  await page.getByRole("button", { name: "Create task", exact: true }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(201);
  const task = taskWire(await response.json());
  expect(task.prompt).toBe(prompt);
  await expect(taskItem(page, prompt)).toBeVisible();
  return task;
}

async function retrySelectedTaskThroughUi(
  page: Page,
  app: LocalApp,
  sourceTaskId: string,
): Promise<TaskWire> {
  const responsePromise = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      response.url() === `${app.origin}/api/tasks/${sourceTaskId}/retry`,
  );
  await page.getByRole("button", { name: "Retry task", exact: true }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(201);
  return taskWire(await response.json());
}

function taskItem(page: Page, prompt: string): Locator {
  return page.locator(".task-list-item").filter({
    has: page.getByText(prompt, { exact: true }),
  });
}

async function selectTask(page: Page, prompt: string): Promise<void> {
  await taskItem(page, prompt).locator("button.task-button").click();
}

async function expectSelectedStatus(page: Page, status: TaskStatus): Promise<void> {
  await expect(page.locator(".task-status-label")).toHaveText(`Execution status: ${status}`, {
    timeout: 20_000,
  });
}

async function expectFailureCode(page: Page, code: string): Promise<void> {
  await expect(page.locator(".failure-panel").getByText(code, { exact: true })).toBeVisible({
    timeout: 20_000,
  });
}

async function expectTaskStatuses(
  page: Page,
  expected: Record<string, TaskStatus>,
): Promise<void> {
  await expect.poll(
    async () => {
      const tasks = await listTasks(page);
      const byId = new Map(tasks.map((task) => [task.id, task.status]));
      return Object.fromEntries(Object.keys(expected).map((id) => [id, byId.get(id) ?? null]));
    },
    { timeout: 20_000, message: "persisted task statuses did not reach the expected state" },
  ).toEqual(expected);
}

async function listTasks(page: Page): Promise<TaskWire[]> {
  const value = await fetchJson(page, "/api/tasks");
  if (!Array.isArray(value)) throw new Error("task list response is not an array");
  return value.map(taskWire);
}

async function taskEventKinds(page: Page, taskId: string): Promise<string[]> {
  const value = await fetchJson(page, `/api/tasks/${encodeURIComponent(taskId)}/events`);
  if (!Array.isArray(value)) throw new Error("task event response is not an array");
  return value.map((event) => {
    if (typeof event !== "object" || event === null || !("kind" in event)) {
      throw new Error("task event response contains an invalid item");
    }
    const kind = (event as TaskEventWire).kind;
    if (typeof kind !== "string") throw new Error("task event kind is not a string");
    return kind;
  });
}

async function fetchJson(page: Page, requestPath: string): Promise<unknown> {
  return page.evaluate(async (path) => {
    const response = await fetch(path, {
      credentials: "same-origin",
      headers: { accept: "application/json" },
    });
    if (!response.ok) {
      throw new Error(`GET ${path} failed with HTTP ${String(response.status)}`);
    }
    return response.json() as Promise<unknown>;
  }, requestPath);
}

function taskWire(value: unknown): TaskWire {
  if (typeof value !== "object" || value === null) {
    throw new Error("task response is not an object");
  }
  const candidate = value as Record<string, unknown>;
  if (
    typeof candidate.id !== "string" ||
    typeof candidate.prompt !== "string" ||
    typeof candidate.status !== "string" ||
    typeof candidate.attempt !== "number"
  ) {
    throw new Error("task response is missing required fields");
  }
  return value as TaskWire;
}

async function assertProcessIsLive(page: Page): Promise<void> {
  await expect(page.locator(".connection-banner")).toContainText("Connected");
  const status = await page.evaluate(async () => (await fetch("/api/bootstrap")).status);
  expect(status).toBe(200);
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
