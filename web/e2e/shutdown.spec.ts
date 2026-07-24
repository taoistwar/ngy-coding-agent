import { randomUUID } from "node:crypto";
import { access } from "node:fs/promises";
import path from "node:path";

import {
  expect,
  test,
  type APIRequestContext,
  type Page,
  type Response,
} from "./fixtures";

import {
  publishReleaseSignal,
  publishUncoordinatedReleaseSignal,
  successScenario,
  waitForReachedSignal,
  withLocalApp,
  type LocalApp,
  type ProcessScenario,
  type ReachedSignal,
} from "./support/localApp";

const API_TIMEOUT_MS = 10_000;
const CLEAN_EXIT_BUDGET_MS = 10_000;

interface TaskView {
  id: string;
  prompt: string;
  status: string;
  failure: { code: string } | null;
}

const emptyScenario = (): ProcessScenario => ({
  ...successScenario(),
  fake_scenarios: [],
});

test("UI quit bounds a runner that ignores cancellation and preserves Interrupted recovery", async (
  { context, page },
  testInfo,
) => {
  await withLocalApp(
    testInfo,
    {
      ...successScenario(),
      fake_scenarios: ["ignores_cancellation"],
    },
    async (app) => {
      await openWorkspace(page, app);
      await addRepositoryThroughUi(page, app);

      const prompt = "Runner ignores cancellation during UI quit";
      const created = await createTaskThroughUi(page, app, prompt);
      await waitForTask(
        context.request,
        app.origin,
        created.id,
        (task) => task.status === "running",
        "the cancellation-ignoring runner to enter Running",
      );
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: running");

      const cleanExitElapsedMs = await quitThroughUi(page, app, CLEAN_EXIT_BUDGET_MS);
      expect(cleanExitElapsedMs).toBeLessThan(CLEAN_EXIT_BUDGET_MS);

      await app.restart(emptyScenario());
      await openWorkspace(page, app);

      const recovered = await waitForTask(
        context.request,
        app.origin,
        created.id,
        (task) => task.status === "interrupted",
        "the task to remain Interrupted after restart",
      );
      expect(recovered.failure?.code).toBe("APP_SHUTDOWN");

      const tasks = await listTasks(context.request, app.origin);
      expect(tasks).toHaveLength(1);
      expect(tasks[0]).toMatchObject({
        id: created.id,
        status: "interrupted",
      });
      expect(tasks.some((task) => task.status === "queued" || task.status === "running")).toBe(
        false,
      );

      const eventKinds = await taskEventKinds(context.request, app.origin, created.id);
      expect(eventKinds.at(-1)).toBe("task.interrupted");
      expect(eventKinds).not.toContain("task.cancelled");

      await page
        .getByRole("button", { name: `${prompt} Attempt 1`, exact: true })
        .click();
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: interrupted");
      await quitThroughUi(page, app, CLEAN_EXIT_BUDGET_MS);
    },
  );
});

test("replays a live completion exactly once after a paused TaskDetail snapshot", async (
  { context, page },
  testInfo,
) => {
  let detailReleasePath = "";
  let runnerReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      detailReleasePath = roots.releaseSignalPath("task-detail-after-snapshot");
      runnerReleasePath = roots.releaseSignalPath("complete-during-task-detail");
      return {
        ...successScenario(),
        fake_scenarios: ["blocking"],
        actor_pauses: ["task_detail_after_snapshot"],
        virtual_release_signals: [
          {
            name: "task-detail-after-snapshot",
            path: detailReleasePath,
            target: "actor_task_detail_after_snapshot",
          },
          {
            name: "complete-during-task-detail",
            path: runnerReleasePath,
            target: "runner_next",
          },
        ],
      };
    },
    async (app) => {
      await openWorkspace(page, app);
      await addRepositoryThroughUi(page, app);

      const prompt = "Complete while TaskDetail is paused";
      const created = await createTaskThroughUi(page, app, prompt);
      if (detailReleasePath.length === 0 || runnerReleasePath.length === 0) {
        throw new Error("TaskDetail joining signals were not configured");
      }
      const detailReached = await waitForReachedSignal(app.runtimeDir, detailReleasePath);

      await publishUncoordinatedReleaseSignal(app.runtimeDir, runnerReleasePath);
      await waitForTask(
        context.request,
        app.origin,
        created.id,
        (task) => task.status === "completed",
        "the live completion while TaskDetail remained paused",
      );
      await publishReleaseSignal(detailReached);

      await expect(page.locator(".task-status-label")).toHaveText("Execution status: completed");
      await expect(
        page.locator(".timeline-panel").getByText("task.completed", { exact: true }),
      ).toHaveCount(1);
      await expect(
        page.getByText("Delivery readiness: review approved", { exact: true }),
      ).toBeVisible();
      await expect(page.getByText("Synthetic checks passed", { exact: true })).toBeVisible();
      await expect(
        page.locator(".review-panel .generation-warning").filter({
          hasText: "Generation mismatch:",
        }),
      ).toHaveCount(0);
      await quitThroughUi(page, app, CLEAN_EXIT_BUDGET_MS);
    },
  );
});

for (const markerWriteFailure of [false, true]) {
  test(`permanent Store failure releases runtime when marker writing is ${
    markerWriteFailure ? "unavailable" : "available"
  }`, async ({ context, page }, testInfo) => {
    await withLocalApp(
      testInfo,
      {
        ...successScenario(),
        fake_scenarios: ["blocking"],
        store_writer_faults: [
          {
            point: "fail_before_execute",
            operation: "recover_incomplete",
            count: 1,
          },
        ],
        marker_write_failure: markerWriteFailure,
      },
      async (app) => {
        await openWorkspace(page, app);
        await addRepositoryThroughUi(page, app);
        const created = await createTaskThroughUi(
          page,
          app,
          `Permanent Store failure marker=${String(!markerWriteFailure)}`,
        );
        await waitForTask(
          context.request,
          app.origin,
          created.id,
          (task) => task.status === "running",
          "the task to enter Running before degraded quit",
        );
        const shuttingDownInstance = await app.runtimeIdentity();
        const markerPath = path.join(
          app.appDataDir,
          `unclean-shutdown.json.${shuttingDownInstance.instanceId}.marker`,
        );

        const startedAt = Date.now();
        await quitThroughUiExpectingExit(page, app, 1, CLEAN_EXIT_BUDGET_MS);
        expect(Date.now() - startedAt).toBeLessThan(CLEAN_EXIT_BUDGET_MS);

        expect(await pathExists(markerPath)).toBe(!markerWriteFailure);

        await app.restart(emptyScenario());
        await openWorkspace(page, app);
        const recovered = await waitForTask(
          context.request,
          app.origin,
          created.id,
          (task) => task.status === "interrupted",
          "startup recovery after degraded quit",
        );
        expect(recovered.failure?.code).toBe("APP_RESTARTED");
        expect(await pathExists(markerPath)).toBe(false);
        await quitThroughUi(page, app, CLEAN_EXIT_BUDGET_MS);
      },
    );
  });
}

test("the UI quit barrier rejects late create and retry before durable interruption", async (
  { context, page },
  testInfo,
) => {
  let quiesceReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots) => {
      quiesceReleasePath = roots.releaseSignalPath("quiesce-before-recovery");
      return {
        ...successScenario(),
        fake_scenarios: ["failure", "blocking"],
        actor_pauses: ["quiesce_before_recovery"],
        virtual_release_signals: [
          {
            name: "quiesce-before-recovery",
            path: quiesceReleasePath,
            target: "actor_quiesce_before_recovery",
          },
        ],
      };
    },
    async (app) => {
      await openWorkspace(page, app);
      const repositoryId = await addRepositoryThroughUi(page, app);
      const failed = await createTaskThroughUi(page, app, "Retry rejected by quit barrier");
      await waitForTask(
        context.request,
        app.origin,
        failed.id,
        (task) => task.status === "failed",
        "the retry source to fail",
      );
      const running = await createTaskThroughUi(page, app, "Interrupted by quit barrier");
      await waitForTask(
        context.request,
        app.origin,
        running.id,
        (task) => task.status === "running",
        "the blocking task to enter Running",
      );
      const csrfToken = await readCsrfToken(context.request, app.origin);
      if (quiesceReleasePath.length === 0) throw new Error("quiesce release was not configured");
      const reachedPromise = waitForReachedSignal(app.runtimeDir, quiesceReleasePath);

      await page.getByRole("button", { name: "Quit local application" }).click();
      const dialog = page.getByRole("dialog", { name: "Quit local application?" });
      await expect(dialog).toBeVisible();
      await dialog.getByRole("button", { name: "Quit application" }).click();
      const reached = await reachedPromise;

      const mutationHeaders = {
        origin: app.origin,
        "x-csrf-token": csrfToken,
      };
      const lateCreate = await context.request.post(`${app.origin}/api/tasks`, {
        headers: mutationHeaders,
        data: {
          client_request_id: randomUUID(),
          repository_id: repositoryId,
          prompt: "Must not cross the quit barrier",
        },
      });
      const lateRetry = await context.request.post(
        `${app.origin}/api/tasks/${failed.id}/retry`,
        { headers: mutationHeaders },
      );
      await expectApiError(lateCreate, 503, "APP_SHUTTING_DOWN");
      await expectApiError(lateRetry, 503, "APP_SHUTTING_DOWN");

      await Promise.all([
        app.waitForCleanExit(CLEAN_EXIT_BUDGET_MS),
        publishReleaseSignal(reached),
      ]);
      await app.restart(emptyScenario());
      await openWorkspace(page, app);
      const tasks = await listTasks(context.request, app.origin);
      expect(tasks).toHaveLength(2);
      expect(tasks.find((task) => task.id === failed.id)?.status).toBe("failed");
      expect(tasks.find((task) => task.id === running.id)).toMatchObject({
        status: "interrupted",
        failure: { code: "APP_SHUTDOWN" },
      });
      expect(tasks.some((task) => task.status === "queued" || task.status === "running")).toBe(
        false,
      );
      await quitThroughUi(page, app, CLEAN_EXIT_BUDGET_MS);
    },
  );
});

test("the quit barrier drains in-flight create and retry before interrupting both", async (
  { context, page },
  testInfo,
) => {
  await withLocalApp(
    testInfo,
    {
      ...successScenario(),
      fake_scenarios: ["failure"],
    },
    async (app) => {
      await openWorkspace(page, app);
      const repositoryId = await addRepositoryThroughUi(page, app);
      const retryPrompt = "Retry already inside the quit barrier";
      const source = await createTaskThroughUi(page, app, retryPrompt);
      await waitForTask(
        context.request,
        app.origin,
        source.id,
        (task) => task.status === "failed",
        "the retry source to fail before the barrier scenario",
      );

      await app.hardKillPrimaryPreservingRoot();
      let createReleasePath = "";
      let retryReleasePath = "";
      await app.restart((roots): ProcessScenario => {
        createReleasePath = roots.releaseSignalPath("inflight-create-before-write");
        retryReleasePath = roots.releaseSignalPath("inflight-retry-before-write");
        return {
          ...successScenario(),
          fake_scenarios: ["blocking", "blocking"],
          actor_pauses: ["create_before_write", "retry_before_write"],
          virtual_release_signals: [
            {
              name: "inflight-create-before-write",
              path: createReleasePath,
              target: "actor_create_before_write",
            },
            {
              name: "inflight-retry-before-write",
              path: retryReleasePath,
              target: "actor_retry_before_write",
            },
          ],
        };
      });
      await openWorkspace(page, app);
      const csrfToken = await readCsrfToken(context.request, app.origin);
      await page.getByRole("button", { name: `${retryPrompt} Attempt 1`, exact: true }).click();
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: failed");

      const createPrompt = "Create already inside the quit barrier";
      await page.getByLabel("Task description").fill(createPrompt);
      await page.getByRole("button", { name: "Create task", exact: true }).click();
      if (createReleasePath.length === 0 || retryReleasePath.length === 0) {
        throw new Error("in-flight mutation release paths were not configured");
      }
      const createReached = await waitForReachedSignal(app.runtimeDir, createReleasePath);

      await page.getByRole("button", { name: "Retry task", exact: true }).click();
      const retryReached = await waitForReachedSignal(app.runtimeDir, retryReleasePath);

      const quitResponse = page.waitForResponse(
        (response) =>
          response.request().method() === "POST" &&
          response.url() === `${app.origin}/api/app/quit`,
      );
      await page.getByRole("button", { name: "Quit local application" }).click();
      const dialog = page.getByRole("dialog", { name: "Quit local application?" });
      await expect(dialog).toBeVisible();
      await dialog.getByRole("button", { name: "Quit application" }).click();
      const accepted = await quitResponse;
      expect(accepted.status()).toBe(202);
      expect(await accepted.finished()).toBeNull();

      const rejectedAfterBarrier = await context.request.post(`${app.origin}/api/tasks`, {
        headers: { origin: app.origin, "x-csrf-token": csrfToken },
        data: {
          client_request_id: randomUUID(),
          repository_id: repositoryId,
          prompt: "Rejected after the in-flight barrier closes",
        },
      });
      await expectApiError(rejectedAfterBarrier, 503, "APP_SHUTTING_DOWN");

      await Promise.all([
        app.waitForCleanExit(CLEAN_EXIT_BUDGET_MS),
        publishReleaseSignal(createReached),
        publishReleaseSignal(retryReached),
      ]);

      await app.restart(emptyScenario());
      await openWorkspace(page, app);
      const tasks = await listTasks(context.request, app.origin);
      expect(tasks).toHaveLength(3);
      expect(tasks.find((task) => task.id === source.id)?.status).toBe("failed");

      const created = tasks.find((task) => task.prompt === createPrompt);
      expect(created).toMatchObject({
        status: "interrupted",
        failure: { code: "APP_SHUTDOWN" },
      });
      const retry = tasks.find(
        (task) => task.prompt === retryPrompt && task.id !== source.id,
      );
      expect(retry).toMatchObject({
        status: "interrupted",
        failure: { code: "APP_SHUTDOWN" },
      });
      expect(tasks.some((task) => task.status === "queued" || task.status === "running")).toBe(
        false,
      );

      await quitThroughUi(page, app, CLEAN_EXIT_BUDGET_MS);
    },
  );
});

test("the quit barrier interrupts a claim paused after handle registration", async (
  { context, page },
  testInfo,
) => {
  let claimReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      claimReleasePath = roots.releaseSignalPath("quit-during-claim-registration");
      return {
        ...successScenario(),
        fake_scenarios: ["blocking"],
        actor_pauses: ["claim_handle_registered"],
        virtual_release_signals: [
          {
            name: "quit-during-claim-registration",
            path: claimReleasePath,
            target: "actor_claim_handle_registered",
          },
        ],
      };
    },
    async (app) => {
      await openWorkspace(page, app);
      await addRepositoryThroughUi(page, app);

      const prompt = "Quit after the claim handle is registered";
      const creation = await beginTaskCreationThroughUi(page, app, prompt);
      if (claimReleasePath.length === 0) {
        throw new Error("claim release path was not configured");
      }
      const claimReached = await waitForReachedSignal(app.runtimeDir, claimReleasePath);
      const created = await waitForTaskByPrompt(context.request, app.origin, prompt);
      await page
        .getByRole("button", { name: `${prompt} Attempt 1`, exact: true })
        .click();
      expect((await readTask(context.request, app.origin, created.id)).status).toBe("queued");
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: queued");

      await quitThroughUiUntilBarrier(page, app);
      const [createdFromResponse] = await Promise.all([
        within(
          creation.response,
          "the paused task-create response after releasing the claim",
        ).then(taskFromResponse),
        releaseActorAndAwaitCleanExit(app, claimReached),
      ]);
      expect(createdFromResponse).toEqual(
        expect.objectContaining({ id: created.id, prompt, status: "queued" }),
      );

      await app.restart(emptyScenario());
      await openWorkspace(page, app);
      const recovered = await waitForTask(
        context.request,
        app.origin,
        created.id,
        (task) => task.status === "interrupted",
        "the paused claim to be interrupted by shutdown",
      );
      expect(recovered).toMatchObject({
        id: created.id,
        status: "interrupted",
        failure: { code: "APP_SHUTDOWN" },
      });

      const tasks = await listTasks(context.request, app.origin);
      expect(tasks).toEqual([
        expect.objectContaining({
          id: created.id,
          status: "interrupted",
          failure: { code: "APP_SHUTDOWN" },
        }),
      ]);
      expect(
        tasks.some(
          (task) =>
            task.status === "queued" ||
            task.status === "running" ||
            task.status === "cancelled",
        ),
      ).toBe(false);

      const eventKinds = await taskEventKinds(context.request, app.origin, created.id);
      expect(eventKinds.at(-1)).toBe("task.interrupted");
      expect(eventKinds).not.toContain("task.cancelled");
      await page
        .getByRole("button", { name: `${prompt} Attempt 1`, exact: true })
        .click();
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: interrupted");
      await quitThroughUi(page, app, CLEAN_EXIT_BUDGET_MS);
    },
  );
});

test("a runner result paused before its write commits before the quit interruption", async (
  { context, page },
  testInfo,
) => {
  let resultReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      resultReleasePath = roots.releaseSignalPath("quit-before-result-write");
      return {
        ...successScenario(),
        fake_scenarios: ["success"],
        actor_pauses: ["result_before_write"],
        virtual_release_signals: [
          {
            name: "quit-before-result-write",
            path: resultReleasePath,
            target: "actor_result_before_write",
          },
        ],
      };
    },
    async (app) => {
      await openWorkspace(page, app);
      await addRepositoryThroughUi(page, app);

      const prompt = "Completion is the first terminal commit during quit";
      const created = await createTaskThroughUi(page, app, prompt);
      if (resultReleasePath.length === 0) {
        throw new Error("result release path was not configured");
      }
      const resultReached = await waitForReachedSignal(app.runtimeDir, resultReleasePath);
      expect((await readTask(context.request, app.origin, created.id)).status).toBe("running");
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: running");

      await quitThroughUiUntilBarrier(page, app);
      await releaseActorAndAwaitCleanExit(app, resultReached);

      await app.restart(emptyScenario());
      await openWorkspace(page, app);
      const terminal = await waitForTask(
        context.request,
        app.origin,
        created.id,
        (task) => task.status === "completed",
        "the paused runner result to win the first terminal commit",
      );
      expect(terminal).toMatchObject({
        id: created.id,
        status: "completed",
        failure: null,
      });

      const tasks = await listTasks(context.request, app.origin);
      expect(tasks).toEqual([
        expect.objectContaining({ id: created.id, status: "completed", failure: null }),
      ]);
      expect(
        tasks.some(
          (task) =>
            task.status === "queued" ||
            task.status === "running" ||
            task.status === "cancelled",
        ),
      ).toBe(false);

      const eventKinds = await taskEventKinds(context.request, app.origin, created.id);
      expect(
        eventKinds.filter(
          (kind) =>
            kind === "task.completed" ||
            kind === "task.failed" ||
            kind === "task.cancelled" ||
            kind === "task.interrupted",
        ),
      ).toEqual(["task.completed"]);
      await page
        .getByRole("button", { name: `${prompt} Attempt 1`, exact: true })
        .click();
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: completed");
      await quitThroughUi(page, app, CLEAN_EXIT_BUDGET_MS);
    },
  );
});

async function openWorkspace(page: Page, app: LocalApp): Promise<void> {
  const bootstrap = page.waitForResponse(
    (response) => response.url() === `${app.origin}/api/bootstrap` && response.status() === 200,
  );
  await app.open(page);
  await bootstrap;
  await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();
  await expect(page.getByRole("status")).toContainText("Connected");
}

async function addRepositoryThroughUi(page: Page, app: LocalApp): Promise<string> {
  await page.getByLabel("Repository path").fill(app.repositoryDir);
  const response = page.waitForResponse(
    (candidate) =>
      candidate.request().method() === "POST" &&
      candidate.url() === `${app.origin}/api/repositories`,
  );
  await page.getByRole("button", { name: "Add repository path" }).click();
  const completed = await response;
  expect(completed.status()).toBe(201);
  const candidate = (await completed.json()) as unknown;
  if (!isRecord(candidate) || typeof candidate.id !== "string") {
    throw new Error("repository response did not contain an ID");
  }
  return candidate.id;
}

async function createTaskThroughUi(
  page: Page,
  app: LocalApp,
  prompt: string,
): Promise<TaskView> {
  await page.getByLabel("Task description").fill(prompt);
  const response = page.waitForResponse(
    (candidate) =>
      candidate.request().method() === "POST" && candidate.url() === `${app.origin}/api/tasks`,
  );
  await page.getByRole("button", { name: "Create task", exact: true }).click();
  return taskFromResponse(await response);
}

async function beginTaskCreationThroughUi(
  page: Page,
  app: LocalApp,
  prompt: string,
): Promise<{ response: Promise<Response> }> {
  await page.getByLabel("Task description").fill(prompt);
  const response = page.waitForResponse(
    (candidate) =>
      candidate.request().method() === "POST" && candidate.url() === `${app.origin}/api/tasks`,
    { timeout: 0 },
  );
  void response.catch(() => undefined);
  await page.getByRole("button", { name: "Create task", exact: true }).click();
  return { response };
}

async function taskFromResponse(response: Response): Promise<TaskView> {
  expect(response.status()).toBe(201);
  return parseTask(await response.json());
}

async function listTasks(api: APIRequestContext, origin: string): Promise<TaskView[]> {
  const response = await api.get(`${origin}/api/tasks`);
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!Array.isArray(candidate)) throw new Error("task list response was not an array");
  return candidate.map(parseTask);
}

async function waitForTaskByPrompt(
  api: APIRequestContext,
  origin: string,
  prompt: string,
): Promise<TaskView> {
  const deadline = Date.now() + API_TIMEOUT_MS;
  while (true) {
    const matches = (await listTasks(api, origin)).filter((task) => task.prompt === prompt);
    if (matches.length === 1) return matches[0]!;
    if (matches.length > 1) throw new Error(`multiple tasks matched prompt ${prompt}`);
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error(`timed out waiting for task prompt ${prompt}`);
    await new Promise((resolve) => setTimeout(resolve, Math.min(25, remaining)));
  }
}

async function within<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), API_TIMEOUT_MS);
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
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

async function readCsrfToken(api: APIRequestContext, origin: string): Promise<string> {
  const response = await api.get(`${origin}/api/bootstrap`);
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!isRecord(candidate) || typeof candidate.csrf_token !== "string") {
    throw new Error("bootstrap response did not contain a CSRF token");
  }
  return candidate.csrf_token;
}

async function expectApiError(
  response: Awaited<ReturnType<APIRequestContext["post"]>>,
  status: number,
  code: string,
): Promise<void> {
  expect(response.status()).toBe(status);
  const candidate = (await response.json()) as unknown;
  if (!isRecord(candidate)) throw new Error("API error response was not an object");
  expect(candidate.code).toBe(code);
  expect(typeof candidate.request_id).toBe("string");
}

async function waitForTask(
  api: APIRequestContext,
  origin: string,
  taskId: string,
  predicate: (task: TaskView) => boolean,
  label: string,
): Promise<TaskView> {
  const deadline = Date.now() + API_TIMEOUT_MS;
  while (true) {
    const task = await readTask(api, origin, taskId);
    if (predicate(task)) return task;
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error(`timed out waiting for ${label}`);
    await new Promise((resolve) => setTimeout(resolve, Math.min(25, remaining)));
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

async function quitThroughUi(page: Page, app: LocalApp, timeoutMs: number): Promise<number> {
  await page.getByRole("button", { name: "Quit local application" }).click();
  const dialog = page.getByRole("dialog", { name: "Quit local application?" });
  await expect(dialog).toBeVisible();
  const startedAt = Date.now();
  await Promise.all([
    app.waitForCleanExit(timeoutMs),
    dialog.getByRole("button", { name: "Quit application" }).click(),
  ]);
  return Date.now() - startedAt;
}

async function quitThroughUiUntilBarrier(page: Page, app: LocalApp): Promise<void> {
  await page.getByRole("button", { name: "Quit local application" }).click();
  const dialog = page.getByRole("dialog", { name: "Quit local application?" });
  await expect(dialog).toBeVisible();
  const response = await responseFromUiAction(
    page,
    (candidate) =>
      candidate.request().method() === "POST" &&
      candidate.url() === `${app.origin}/api/app/quit`,
    () => dialog.getByRole("button", { name: "Quit application" }).click(),
  );
  expect(response.status()).toBe(202);
  expect(await response.json()).toEqual({ status: "shutting_down" });
  expect(await response.finished()).toBeNull();
}

async function responseFromUiAction(
  page: Page,
  predicate: (response: Response) => boolean,
  action: () => Promise<unknown>,
): Promise<Response> {
  type Outcome =
    | { kind: "response"; response: Response }
    | { kind: "error"; error: Error }
    | { kind: "cancelled" };

  let settled = false;
  let settle: (outcome: Outcome) => void = () => undefined;
  const outcome = new Promise<Outcome>((resolve) => {
    settle = resolve;
  });
  const finish = (value: Outcome): void => {
    if (settled) return;
    settled = true;
    settle(value);
  };
  const onResponse = (response: Response): void => {
    try {
      if (predicate(response)) finish({ kind: "response", response });
    } catch (error) {
      finish({
        kind: "error",
        error: error instanceof Error ? error : new Error(String(error)),
      });
    }
  };
  const timer = setTimeout(
    () => finish({ kind: "error", error: new Error("timed out waiting for the UI response") }),
    API_TIMEOUT_MS,
  );
  page.on("response", onResponse);
  try {
    await action();
    const completed = await outcome;
    if (completed.kind === "response") return completed.response;
    if (completed.kind === "error") throw completed.error;
    throw new Error("UI response wait was cancelled");
  } catch (error) {
    finish({ kind: "cancelled" });
    throw error;
  } finally {
    clearTimeout(timer);
    page.off("response", onResponse);
  }
}

async function releaseActorAndAwaitCleanExit(
  app: LocalApp,
  reached: ReachedSignal,
): Promise<void> {
  let released = false;
  try {
    await publishReleaseSignal(reached);
    released = true;
    await app.waitForCleanExit(CLEAN_EXIT_BUDGET_MS);
  } finally {
    if (!released) {
      await publishReleaseSignal(reached).catch(() => undefined);
    }
  }
}

async function quitThroughUiExpectingExit(
  page: Page,
  app: LocalApp,
  expectedExitCode: number,
  timeoutMs: number,
): Promise<void> {
  await page.getByRole("button", { name: "Quit local application" }).click();
  const dialog = page.getByRole("dialog", { name: "Quit local application?" });
  await expect(dialog).toBeVisible();
  await Promise.all([
    app.waitForExitCode(expectedExitCode, timeoutMs),
    dialog.getByRole("button", { name: "Quit application" }).click(),
  ]);
}

async function pathExists(candidate: string): Promise<boolean> {
  try {
    await access(candidate);
    return true;
  } catch {
    return false;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
