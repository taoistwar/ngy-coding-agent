import {
  expect,
  test,
  type APIRequestContext,
  type Page,
  type Request,
  type Response,
} from "./fixtures";

import {
  publishReleaseSignal,
  publishUncoordinatedReleaseSignal,
  waitForReachedSignal,
  withLocalApp,
  type LocalApp,
  type ProcessScenario,
} from "./support/localApp";

const API_TIMEOUT_MS = 15_000;
const CLEAN_EXIT_BUDGET_MS = 10_000;
const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;

type TaskStatus =
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "interrupted";

interface TaskView {
  id: string;
  prompt: string;
  status: TaskStatus;
}

interface CreateBody {
  client_request_id: string;
  repository_id: string;
  prompt: string;
}

test("keeps a Running task alive across reload, page close, and a fresh reopen", async (
  { context, page },
  testInfo,
) => {
  let runnerReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      runnerReleasePath = roots.releaseSignalPath("complete-after-reopen");
      return {
        runtime_config: null,
        fake_scenarios: ["blocking"],
        storage_samples: [{ kind: "native" }],
        store_writer_faults: [],
        actor_pauses: [],
        virtual_release_signals: [
          {
            name: "complete-after-reopen",
            path: runnerReleasePath,
            target: "runner_next",
          },
        ],
        legacy_v2_seed: { kind: "none" },
        marker_write_failure: false,
      };
    },
    async (app) => {
      await openWorkspace(page, app, () => app.open(page));
      await addRepositoryThroughUi(page, app);

      const prompt = "Keep running while every browser page is replaced";
      const created = await createTaskThroughUi(page, app, prompt);
      await waitForTaskStatus(context.request, app.origin, created.id, "running");
      await expectSelectedStatus(page, "running");

      await reloadWorkspace(page, app);
      await selectTask(page, prompt);
      await expectSelectedStatus(page, "running");
      expect((await readTask(context.request, app.origin, created.id)).status).toBe("running");

      await page.close();
      const reopenedPage = await context.newPage();
      await openWorkspace(reopenedPage, app, () => app.reopen(reopenedPage));
      await selectTask(reopenedPage, prompt);
      await expectSelectedStatus(reopenedPage, "running");
      expect((await readTask(context.request, app.origin, created.id)).status).toBe("running");

      if (runnerReleasePath.length === 0) {
        throw new Error("runner release path was not initialized");
      }
      await publishUncoordinatedReleaseSignal(app.runtimeDir, runnerReleasePath);
      await waitForTaskStatus(context.request, app.origin, created.id, "completed");
      await expectSelectedStatus(reopenedPage, "completed");

      const eventKinds = await taskEventKinds(context.request, app.origin, created.id);
      expect(eventKinds).toEqual([
        "task.queued",
        "task.started",
        "plan.updated",
        "diff.updated",
        "test.updated",
        "review.updated",
        "task.completed",
      ]);
      for (const kind of eventKinds.filter((value) => value.startsWith("task."))) {
        await expect(
          reopenedPage.locator(".timeline-panel").getByText(kind, { exact: true }),
        ).toHaveCount(1);
      }
      expect((await listTasks(context.request, app.origin))).toEqual([
        expect.objectContaining({ id: created.id, status: "completed" }),
      ]);

      await assertProcessIsConnected(reopenedPage, app);
      await quitThroughUi(reopenedPage, app);
    },
  );
});

test("renders STORE_BUSY and reuses one create UUID for an explicit successful retry", async (
  { context, page },
  testInfo,
) => {
  // StoreWriter makes one initial attempt plus one attempt after each of its
  // five fixed retry delays. Six injected Busy results exhaust that window.
  const scenario: ProcessScenario = {
    runtime_config: null,
    fake_scenarios: ["success"],
    storage_samples: [{ kind: "native" }],
    store_writer_faults: [
      {
        point: "busy_before_execute",
        operation: "create_task",
        count: 6,
      },
    ],
    actor_pauses: [],
    virtual_release_signals: [],
    legacy_v2_seed: { kind: "none" },
    marker_write_failure: false,
  };

  await withLocalApp(testInfo, scenario, async (app) => {
    await openWorkspace(page, app, () => app.open(page));
    await addRepositoryThroughUi(page, app);

    const prompt = "Retry the same ambiguous create after STORE_BUSY";
    await page.getByLabel("Task description").fill(prompt);

    const firstResponsePromise = createResponse(page, app);
    await page.getByRole("button", { name: "Create task", exact: true }).click();
    const firstResponse = await firstResponsePromise;
    const firstBody = createBody(firstResponse.request());
    expect(firstResponse.status()).toBe(503);
    const busyError = await apiError(firstResponse);
    expect(busyError).toEqual({
      code: "STORE_BUSY",
      details: {},
      message: "the local store is busy; retry the request",
      request_id: expect.stringMatching(UUID_V4),
      retryable: true,
    });

    const alert = page.getByRole("alert");
    await expect(alert).toContainText("the local store is busy; retry the request");
    await expect(alert).toContainText("Error code: STORE_BUSY");
    await expect(
      alert.getByText(`Request ID: ${busyError.request_id}`, { exact: true }),
    ).toBeVisible();
    await expect(
      alert.getByText(`Client request ID: ${firstBody.client_request_id}`, { exact: true }),
    ).toBeVisible();
    expect(firstBody.prompt).toBe(prompt);
    expect(firstBody.client_request_id).toMatch(UUID_V4);
    expect(await listTasks(context.request, app.origin)).toEqual([]);

    const secondResponsePromise = createResponse(page, app);
    await page.getByRole("button", { name: "Retry create task", exact: true }).click();
    const secondResponse = await secondResponsePromise;
    const secondBody = createBody(secondResponse.request());
    expect(secondResponse.status()).toBe(201);
    expect(secondBody).toEqual(firstBody);
    const created = parseTask(await secondResponse.json());

    await waitForTaskStatus(context.request, app.origin, created.id, "completed");
    await expectSelectedStatus(page, "completed");
    expect(await listTasks(context.request, app.origin)).toEqual([
      expect.objectContaining({ id: created.id, prompt, status: "completed" }),
    ]);
    const terminalKinds = (await taskEventKinds(context.request, app.origin, created.id))
      .filter(isTerminalEvent);
    expect(terminalKinds).toEqual(["task.completed"]);

    await assertProcessIsConnected(page, app);
    await quitThroughUi(page, app);
  });
});

test("replays the same create UUID after the first response is lost post-commit", async (
  { context, page },
  testInfo,
) => {
  let writerReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      writerReleasePath = roots.releaseSignalPath("create-committed-response-lost");
      return {
        runtime_config: null,
        fake_scenarios: ["success"],
        storage_samples: [{ kind: "native" }],
        store_writer_faults: [
          {
            point: "pause_after_commit_before_wake",
            operation: "create_task",
            count: 1,
          },
        ],
        actor_pauses: [],
        virtual_release_signals: [
          {
            name: "create-committed-response-lost",
            path: writerReleasePath,
            target: "store_writer_after_commit_before_wake",
          },
        ],
        legacy_v2_seed: { kind: "none" },
        marker_write_failure: false,
      };
    },
    async (app) => {
      await openWorkspace(page, app, () => app.open(page));
      await addRepositoryThroughUi(page, app);

      const prompt = "Replay one committed create after its browser response disappears";
      await page.getByLabel("Task description").fill(prompt);
      const firstRequestPromise = page.waitForRequest(
        (request) =>
          request.method() === "POST" && request.url() === `${app.origin}/api/tasks`,
      );
      await page.getByRole("button", { name: "Create task", exact: true }).click();
      const firstRequest = await firstRequestPromise;
      const firstBody = createBody(firstRequest);
      const csrfToken = firstRequest.headers()["x-csrf-token"];
      if (csrfToken === undefined || csrfToken.length === 0) {
        throw new Error("the UI create request did not carry a CSRF token");
      }

      const committed = await waitForOnlyTask(
        context.request,
        app.origin,
        firstBody.client_request_id,
      );
      expect(committed.prompt).toBe(prompt);
      expect(committed.status).toBe("queued");
      if (writerReleasePath.length === 0) {
        throw new Error("writer release path was not initialized");
      }
      const writerReached = await waitForReachedSignal(
        app.runtimeDir,
        writerReleasePath,
      );

      // The browser disappears while StoreWriter is paused after its durable
      // commit but before the response can be completed.
      await page.close();
      await publishReleaseSignal(writerReached);

      const replay = await context.request.post(`${app.origin}/api/tasks`, {
        data: firstBody,
        headers: { origin: app.origin, "x-csrf-token": csrfToken },
      });
      expect(replay.status()).toBe(200);
      const replayed = parseTask(await replay.json());
      await replay.dispose();
      expect(replayed.id).toBe(committed.id);

      const reopenedPage = await context.newPage();
      await openWorkspace(reopenedPage, app, () => app.reopen(reopenedPage));
      await waitForTaskStatus(context.request, app.origin, committed.id, "completed");
      await selectTask(reopenedPage, prompt);
      await expectSelectedStatus(reopenedPage, "completed");

      const tasks = await listTasks(context.request, app.origin);
      expect(tasks).toEqual([
        expect.objectContaining({ id: committed.id, prompt, status: "completed" }),
      ]);
      const eventKinds = await taskEventKinds(context.request, app.origin, committed.id);
      expect(eventKinds.filter((kind) => kind === "task.queued")).toHaveLength(1);
      expect(eventKinds.filter((kind) => kind === "task.started")).toHaveLength(1);
      expect(eventKinds.filter(isTerminalEvent)).toEqual(["task.completed"]);

      await assertProcessIsConnected(reopenedPage, app);
      await quitThroughUi(reopenedPage, app);
    },
  );
});

async function openWorkspace(
  page: Page,
  app: LocalApp,
  navigate: () => Promise<void>,
): Promise<void> {
  const bootstrap = page.waitForResponse(
    (response) => response.url() === `${app.origin}/api/bootstrap` && response.status() === 200,
  );
  await navigate();
  await bootstrap;
  await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();
  await expect(page.getByRole("status")).toContainText("Connected");
}

async function reloadWorkspace(page: Page, app: LocalApp): Promise<void> {
  const bootstrap = page.waitForResponse(
    (response) => response.url() === `${app.origin}/api/bootstrap` && response.status() === 200,
  );
  await page.reload({ waitUntil: "domcontentloaded" });
  await bootstrap;
  await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();
  await expect(page.getByRole("status")).toContainText("Connected");
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
  await response.body();
}

async function createTaskThroughUi(
  page: Page,
  app: LocalApp,
  prompt: string,
): Promise<TaskView> {
  await page.getByLabel("Task description").fill(prompt);
  const responsePromise = createResponse(page, app);
  await page.getByRole("button", { name: "Create task", exact: true }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(201);
  return parseTask(await response.json());
}

function createResponse(page: Page, app: LocalApp): Promise<Response> {
  return page.waitForResponse(
    (response) =>
      response.request().method() === "POST" && response.url() === `${app.origin}/api/tasks`,
  );
}

function createBody(request: Request): CreateBody {
  const candidate = request.postDataJSON() as unknown;
  if (
    !isRecord(candidate) ||
    typeof candidate.client_request_id !== "string" ||
    typeof candidate.repository_id !== "string" ||
    typeof candidate.prompt !== "string" ||
    !hasExactKeys(candidate, ["client_request_id", "prompt", "repository_id"])
  ) {
    throw new Error("create request body was invalid");
  }
  return {
    client_request_id: candidate.client_request_id,
    repository_id: candidate.repository_id,
    prompt: candidate.prompt,
  };
}

async function apiError(response: Response): Promise<{
  code: string;
  details: Record<string, unknown>;
  message: string;
  request_id: string;
  retryable: boolean;
}> {
  const candidate = (await response.json()) as unknown;
  if (
    !isRecord(candidate) ||
    typeof candidate.code !== "string" ||
    !isRecord(candidate.details) ||
    typeof candidate.message !== "string" ||
    typeof candidate.request_id !== "string" ||
    typeof candidate.retryable !== "boolean" ||
    !hasExactKeys(candidate, ["code", "details", "message", "request_id", "retryable"])
  ) {
    throw new Error("API error response was invalid");
  }
  return {
    code: candidate.code,
    details: candidate.details,
    message: candidate.message,
    request_id: candidate.request_id,
    retryable: candidate.retryable,
  };
}

async function readTask(
  api: APIRequestContext,
  origin: string,
  taskId: string,
): Promise<TaskView> {
  const response = await api.get(`${origin}/api/tasks/${encodeURIComponent(taskId)}`);
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!isRecord(candidate)) throw new Error("task detail response was not an object");
  return parseTask(candidate.task);
}

async function listTasks(api: APIRequestContext, origin: string): Promise<TaskView[]> {
  const response = await api.get(`${origin}/api/tasks`);
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!Array.isArray(candidate)) throw new Error("task list response was not an array");
  return candidate.map(parseTask);
}

async function taskEventKinds(
  api: APIRequestContext,
  origin: string,
  taskId: string,
): Promise<string[]> {
  const response = await api.get(
    `${origin}/api/tasks/${encodeURIComponent(taskId)}/events?after=0`,
  );
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!Array.isArray(candidate)) throw new Error("task events response was not an array");
  return candidate.map((event) => {
    if (!isRecord(event) || typeof event.kind !== "string") {
      throw new Error("task event response contained an invalid entry");
    }
    return event.kind;
  });
}

async function waitForTaskStatus(
  api: APIRequestContext,
  origin: string,
  taskId: string,
  status: TaskStatus,
): Promise<TaskView> {
  const deadline = Date.now() + API_TIMEOUT_MS;
  while (true) {
    const task = await readTask(api, origin, taskId);
    if (task.status === status) return task;
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      throw new Error(`timed out waiting for task status ${status}; last=${task.status}`);
    }
    await new Promise((resolve) => setTimeout(resolve, Math.min(25, remaining)));
  }
}

async function waitForOnlyTask(
  api: APIRequestContext,
  origin: string,
  clientRequestId: string,
): Promise<TaskView> {
  if (!UUID_V4.test(clientRequestId)) {
    throw new Error("the UI create request did not carry a canonical UUID");
  }
  const deadline = Date.now() + API_TIMEOUT_MS;
  while (true) {
    const tasks = await listTasks(api, origin);
    if (tasks.length === 1) return tasks[0]!;
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      throw new Error(`timed out waiting for one committed task; observed=${tasks.length}`);
    }
    await new Promise((resolve) => setTimeout(resolve, Math.min(25, remaining)));
  }
}

async function selectTask(page: Page, prompt: string): Promise<void> {
  await page.getByRole("button", { name: `${prompt} Attempt 1`, exact: true }).click();
}

async function expectSelectedStatus(page: Page, status: TaskStatus): Promise<void> {
  await expect(page.locator(".task-status-label")).toHaveText(`Execution status: ${status}`);
}

async function assertProcessIsConnected(page: Page, app: LocalApp): Promise<void> {
  await expect(page.getByRole("status")).toContainText("Connected");
  const response = await page.request.get(`${app.origin}/api/bootstrap`);
  expect(response.status()).toBe(200);
  await response.dispose();
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

function parseTask(candidate: unknown): TaskView {
  if (
    !isRecord(candidate) ||
    typeof candidate.id !== "string" ||
    typeof candidate.prompt !== "string" ||
    !isTaskStatus(candidate.status)
  ) {
    throw new Error("task response contained invalid identity or status fields");
  }
  return {
    id: candidate.id,
    prompt: candidate.prompt,
    status: candidate.status,
  };
}

function isTaskStatus(candidate: unknown): candidate is TaskStatus {
  return (
    candidate === "queued" ||
    candidate === "running" ||
    candidate === "completed" ||
    candidate === "failed" ||
    candidate === "cancelled" ||
    candidate === "interrupted"
  );
}

function isTerminalEvent(kind: string): boolean {
  return (
    kind === "task.completed" ||
    kind === "task.failed" ||
    kind === "task.cancelled" ||
    kind === "task.interrupted"
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, expected: string[]): boolean {
  const actual = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  return (
    actual.length === sortedExpected.length &&
    actual.every((key, index) => key === sortedExpected[index])
  );
}
