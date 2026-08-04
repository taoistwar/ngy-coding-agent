import { expect, test, type Page, type Route } from "./fixtures";

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
  repository_id: string;
  client_request_id: string;
  prompt: string;
  status: TaskStatus;
}

interface ApiErrorWire {
  code: string;
  message: string;
  retryable: boolean;
  request_id: string;
  details: Record<string, unknown>;
}

interface CreateTaskWire {
  client_request_id: string;
  repository_id: string;
  prompt: string;
}

const FIRST_PROMPT = "occupy the only running slot";
const SECOND_PROMPT = "occupy the second running slot";
const REPLAY_PROMPT = "replay this exact queue-full request";
const QUEUE_LIMIT = 32;

test("retains and replays one exact command after a real queue-full response", async (
  { page },
  testInfo,
) => {
  test.setTimeout(120_000);
  let runnerReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      runnerReleasePath = roots.releaseSignalPath("release-running-task");
      return {
        runtime_config: null,
        fake_scenarios: ["blocking", "blocking", "blocking"],
        storage_samples: [{ kind: "native" }],
        store_writer_faults: [],
        actor_pauses: [],
        virtual_release_signals: [
          {
            name: "release-running-task",
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

      await createTaskThroughUi(page, app, FIRST_PROMPT);
      await expectTaskStatus(page, FIRST_PROMPT, "running");
      await createTaskThroughUi(page, app, SECOND_PROMPT);
      await expectTaskStatus(page, SECOND_PROMPT, "running");
      for (let index = 1; index < QUEUE_LIMIT; index += 1) {
        await createTaskThroughUi(page, app, queuePrompt(index));
      }
      await expectTaskStatus(page, queuePrompt(QUEUE_LIMIT - 1), "queued");
      await expect(
        page.getByText(`${String(QUEUE_LIMIT - 1)} / ${String(QUEUE_LIMIT)} queued`, {
          exact: true,
        }),
      ).toBeVisible();

      const abortEventStream = (route: Route) => route.abort("connectionrefused");
      await page.route("**/api/events*", abortEventStream);
      await page.reload();
      await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();
      await expect(page.locator(".connection-reconnecting")).toBeVisible();

      await createTaskThroughUi(page, app, queuePrompt(QUEUE_LIMIT));
      await expectTaskStatus(page, queuePrompt(QUEUE_LIMIT), "queued");
      await expect(
        page.getByText(`${String(QUEUE_LIMIT - 1)} / ${String(QUEUE_LIMIT)} queued`, {
          exact: true,
        }),
      ).toBeVisible();

      const input = page.getByLabel("Task description");
      await input.fill(REPLAY_PROMPT);
      const createButton = page.getByRole("button", { name: "Create task", exact: true });
      await expect(createButton).toBeEnabled();

      const queueFullResponsePromise = page.waitForResponse(
        (response) =>
          response.request().method() === "POST" &&
          response.url() === `${app.origin}/api/tasks`,
      );
      await createButton.click();
      const queueFullResponse = await queueFullResponsePromise;
      const firstCommand = createTaskWire(queueFullResponse.request().postDataJSON());
      const queueFull = apiErrorWire(await queueFullResponse.json());

      expect(queueFullResponse.status()).toBe(429);
      expect(queueFullResponse.headers()["retry-after"]).toBeUndefined();
      expect(queueFull).toEqual({
        code: "TASK_QUEUE_FULL",
        message: "the task queue is full; retry after capacity becomes available",
        retryable: true,
        request_id: expect.any(String),
        details: {
          max_queued_tasks: QUEUE_LIMIT,
          queued_tasks: QUEUE_LIMIT,
        },
      });
      expect(firstCommand).toMatchObject({
        prompt: REPLAY_PROMPT,
        client_request_id: expect.any(String),
      });

      await expect(input).toHaveValue(REPLAY_PROMPT);
      await expect(page.getByRole("alert")).toContainText(queueFull.message);
      await expect(
        page.getByText(`Request ID: ${queueFull.request_id}`, { exact: true }),
      ).toBeVisible();
      await expect(
        page.getByText(`Client request ID: ${firstCommand.client_request_id}`, {
          exact: true,
        }),
      ).toBeVisible();
      await expect(
        page.getByRole("button", { name: "Retry create task", exact: true }),
      ).toBeDisabled();
      expect(await listTasks(page)).toHaveLength(QUEUE_LIMIT + 2);
      expect(
        (await listTasks(page)).some(
          (task) => task.client_request_id === firstCommand.client_request_id,
        ),
      ).toBe(false);

      if (runnerReleasePath.length === 0) {
        throw new Error("runner release path was not initialized");
      }
      await publishUncoordinatedReleaseSignal(app.runtimeDir, runnerReleasePath);
      await expectTaskStatus(page, FIRST_PROMPT, "completed");
      await expectTaskStatus(page, queuePrompt(1), "running");

      await page.unroute("**/api/events*", abortEventStream);
      await expect(page.locator(".connection-connected")).toBeVisible();
      await expect(
        page.getByText(`${String(QUEUE_LIMIT - 1)} / ${String(QUEUE_LIMIT)} queued`, {
          exact: true,
        }),
      ).toBeVisible();

      const retryButton = page.getByRole("button", {
        name: "Retry create task",
        exact: true,
      });
      await expect(retryButton).toBeEnabled();
      const replayResponsePromise = page.waitForResponse(
        (response) =>
          response.request().method() === "POST" &&
          response.url() === `${app.origin}/api/tasks`,
      );
      await retryButton.click();
      const replayResponse = await replayResponsePromise;
      const replayCommand = createTaskWire(replayResponse.request().postDataJSON());
      const replayedTask = taskWire(await replayResponse.json());

      expect(replayResponse.status()).toBe(201);
      expect(replayCommand).toEqual(firstCommand);
      expect(replayedTask.client_request_id).toBe(firstCommand.client_request_id);
      await expect(input).toHaveValue("");
      await expectTaskStatus(page, REPLAY_PROMPT, "queued");

      const finalTasks = await listTasks(page);
      expect(finalTasks).toHaveLength(QUEUE_LIMIT + 3);
      expect(
        finalTasks.filter(
          (task) => task.client_request_id === firstCommand.client_request_id,
        ),
      ).toEqual([replayedTask]);

      await quitThroughUi(page, app);
    },
  );
});

function queuePrompt(index: number): string {
  return `fill durable queue slot ${String(index)}`;
}

async function openWorkspace(page: Page, app: LocalApp): Promise<void> {
  await app.open(page);
  await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();
  await expect(page.locator(".connection-connected")).toBeVisible();
}

async function addRepositoryThroughUi(page: Page, app: LocalApp): Promise<void> {
  await page.getByLabel("Repository path").fill(app.repositoryDir);
  const responsePromise = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      response.url() === `${app.origin}/api/repositories`,
  );
  await page.getByRole("button", { name: "Add repository path" }).click();
  expect((await responsePromise).status()).toBe(201);
}

async function createTaskThroughUi(
  page: Page,
  app: LocalApp,
  prompt: string,
): Promise<TaskWire> {
  await page.getByLabel("Task description").fill(prompt);
  const responsePromise = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      response.url() === `${app.origin}/api/tasks`,
  );
  await page.getByRole("button", { name: "Create task", exact: true }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(201);
  return taskWire(await response.json());
}

async function expectTaskStatus(
  page: Page,
  prompt: string,
  status: TaskStatus,
): Promise<void> {
  await expect
    .poll(
      async () => (await listTasks(page)).find((task) => task.prompt === prompt)?.status,
      { timeout: 20_000 },
    )
    .toBe(status);
}

async function listTasks(page: Page): Promise<TaskWire[]> {
  const value = await page.evaluate(async () => {
    const response = await fetch("/api/tasks", {
      credentials: "same-origin",
      headers: { accept: "application/json" },
    });
    if (!response.ok) {
      throw new Error(`task list failed with HTTP ${String(response.status)}`);
    }
    return response.json() as Promise<unknown>;
  });
  if (!Array.isArray(value)) throw new Error("task list response is not an array");
  return value.map(taskWire);
}

function taskWire(value: unknown): TaskWire {
  if (
    !isRecord(value) ||
    typeof value.id !== "string" ||
    typeof value.repository_id !== "string" ||
    typeof value.client_request_id !== "string" ||
    typeof value.prompt !== "string" ||
    typeof value.status !== "string"
  ) {
    throw new Error("task response is invalid");
  }
  return value as unknown as TaskWire;
}

function createTaskWire(value: unknown): CreateTaskWire {
  if (
    !isRecord(value) ||
    typeof value.client_request_id !== "string" ||
    typeof value.repository_id !== "string" ||
    typeof value.prompt !== "string"
  ) {
    throw new Error("create-task request is invalid");
  }
  return value as unknown as CreateTaskWire;
}

function apiErrorWire(value: unknown): ApiErrorWire {
  if (
    !isRecord(value) ||
    typeof value.code !== "string" ||
    typeof value.message !== "string" ||
    typeof value.retryable !== "boolean" ||
    typeof value.request_id !== "string" ||
    !isRecord(value.details)
  ) {
    throw new Error("API error response is invalid");
  }
  return value as unknown as ApiErrorWire;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
