import { randomUUID } from "node:crypto";

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
} from "./support/localApp";

const API_TIMEOUT_MS = 10_000;
const STREAM_TIMEOUT_MS = 10_000;

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

interface CapturedSseFrame {
  raw: string;
  event: string | null;
  id: string | null;
  data: unknown;
}

test("a dropped create wake converges through the dispatcher poll exactly once", async (
  { context, page },
  testInfo,
) => {
  let startReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      startReleasePath = roots.releaseSignalPath("release-paused-start-after-poll");
      return {
        ...successScenario(),
        fake_scenarios: ["blocking"],
        store_writer_faults: [
          {
            point: "drop_wake_after_commit",
            operation: "create_task",
            count: 1,
          },
          {
            point: "pause_before_execute",
            operation: "start_task",
            count: 1,
          },
        ],
        virtual_release_signals: [
          {
            name: "release-paused-start-after-poll",
            path: startReleasePath,
            target: "store_writer_before_execute",
          },
        ],
      };
    },
    async (app) => {
      await openWorkspace(page, app);
      await addRepositoryThroughUi(page, app);

      const cursorBeforeCreate = await bootstrapCursor(context.request, app.origin);
      const observer = await context.newPage();
      const observerBootstrap = await observer.goto(`${app.origin}/api/bootstrap`);
      expect(observerBootstrap?.status()).toBe(200);

      const streamUrl = `${app.origin}/api/events?after=${String(cursorBeforeCreate)}`;
      const streamHeaders = observer.waitForResponse(
        (response) => response.url() === streamUrl && response.request().resourceType() === "fetch",
      );
      const queuedFramePromise = readLiveFrame(observer, cursorBeforeCreate, "task.queued");
      const response = await streamHeaders;
      expect(response.status()).toBe(200);
      expect(response.headers()["content-type"]).toContain("text/event-stream");

      const prompt = "Dispatcher poll recovers a dropped create wake";
      const created = await createTaskThroughUi(page, app, prompt);
      const queuedFrame = await queuedFramePromise;
      expect(queuedFrame.event).toBe("task.queued");
      expect(Number(queuedFrame.id)).toBeGreaterThan(cursorBeforeCreate);
      expect(queuedFrame.data).toMatchObject({
        id: Number(queuedFrame.id),
        kind: "task.queued",
        task_id: created.id,
      });

      const stillQueued = await readTask(context.request, app.origin, created.id);
      expect(stillQueued.status).toBe("queued");
      const queuedEvents = await taskEvents(context.request, app.origin, created.id);
      expect(queuedEvents.filter((event) => event.kind === "task.queued")).toHaveLength(1);
      await expect(
        page.locator(".timeline-panel").getByText("task.queued", { exact: true }),
      ).toHaveCount(1);

      if (startReleasePath.length === 0) throw new Error("start release path was not configured");
      await publishUncoordinatedReleaseSignal(app.runtimeDir, startReleasePath);
      await waitForTask(
        context.request,
        app.origin,
        created.id,
        (task) => task.status === "running",
        "the released task to enter Running",
      );
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: running");
      await page.getByRole("button", { name: "Cancel task", exact: true }).click();
      await waitForTask(
        context.request,
        app.origin,
        created.id,
        (task) => task.status === "cancelled",
        "the blocking task to cancel",
      );
      await expect(page.locator(".task-status-label")).toHaveText("Execution status: cancelled");

      const finalEvents = await taskEvents(context.request, app.origin, created.id);
      expect(finalEvents.filter((event) => event.kind === "task.queued")).toHaveLength(1);
      await observer.close();
      await quitThroughUi(page, app);
    },
  );
});

test("an ahead cursor receives an id-less reset and the normal UI stream reconnects", async (
  { page },
  testInfo,
) => {
  let releasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      releasePath = roots.releaseSignalPath("bootstrap-cursor-ahead");
      return {
        ...successScenario(),
        actor_pauses: ["bootstrap_cursor_ahead"],
        virtual_release_signals: [
          {
            name: "bootstrap-cursor-ahead",
            path: releasePath,
            target: "actor_bootstrap_cursor_ahead",
          },
        ],
      };
    },
    async (app) => {
      let bootstrapResponses = 0;
      let mainFrameNavigations = 0;
      page.on("framenavigated", (frame) => {
        if (frame === page.mainFrame()) mainFrameNavigations += 1;
      });

      const recoveryBootstrap = page.waitForResponse((response) => {
        if (response.url() !== `${app.origin}/api/bootstrap` || response.status() !== 200) {
          return false;
        }
        bootstrapResponses += 1;
        return bootstrapResponses === 2;
      });
      const resetStream = page.waitForResponse((response) => {
        const url = new URL(response.url());
        return (
          url.origin === app.origin &&
          url.pathname === "/api/events" &&
          url.searchParams.get("after") === "1"
        );
      });
      const recoveredStream = page.waitForRequest((request) => {
        const url = new URL(request.url());
        return (
          url.origin === app.origin &&
          url.pathname === "/api/events" &&
          url.searchParams.get("after") === "0"
        );
      });

      await app.open(page);
      const navigationCountAtResetBoundary = mainFrameNavigations;
      if (releasePath.length === 0) throw new Error("bootstrap release path was not configured");
      const reached = await waitForReachedSignal(app.runtimeDir, releasePath);
      await publishReleaseSignal(reached);

      const response = await resetStream;
      expect(response.status()).toBe(200);
      expect(response.headers()["content-type"]).toContain("text/event-stream");

      await recoveryBootstrap;
      const streamRequest = await recoveredStream;
      expect(new URL(streamRequest.url()).searchParams.get("after")).toBe("0");
      await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();
      await expect(page.locator(".connection-banner")).toContainText("Connected");
      expect(bootstrapResponses).toBe(2);
      expect(mainFrameNavigations).toBe(navigationCountAtResetBoundary);

      await addRepositoryThroughUi(page, app);
      await quitThroughUi(page, app);
    },
  );
});

test("a newer service control cannot regress behind a paused bootstrap snapshot", async (
  { context, page },
  testInfo,
) => {
  let bootstrapReleasePath = "";
  let recoveryReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      bootstrapReleasePath = roots.releaseSignalPath("bootstrap-before-service-change");
      recoveryReleasePath = roots.releaseSignalPath("recovery-after-service-change");
      return {
        ...successScenario(),
        fake_scenarios: ["failure"],
        store_writer_faults: [
          {
            point: "fail_before_execute",
            operation: "finish_task",
            count: 1,
          },
          {
            point: "pause_before_execute",
            operation: "recover_incomplete",
            count: 1,
          },
        ],
        actor_pauses: ["bootstrap_before_sse"],
        virtual_release_signals: [
          {
            name: "bootstrap-before-service-change",
            path: bootstrapReleasePath,
            target: "actor_bootstrap_before_sse",
          },
          {
            name: "recovery-after-service-change",
            path: recoveryReleasePath,
            target: "store_writer_before_execute",
          },
        ],
      };
    },
    async (app) => {
      await app.open(page);
      if (bootstrapReleasePath.length === 0 || recoveryReleasePath.length === 0) {
        throw new Error("service-order release paths were not configured");
      }
      const bootstrapReached = await waitForReachedSignal(
        app.runtimeDir,
        bootstrapReleasePath,
      );

      // The UI owns the first, paused bootstrap. A second authorized read is
      // allowed through so the test can create real work while that stale
      // Ready snapshot remains suspended.
      const csrfToken = await readCsrfToken(context.request, app.origin);
      const repositoryId = await addRepositoryThroughApi(
        context.request,
        app,
        csrfToken,
      );
      const prompt = "Advance service generation behind bootstrap";
      const created = await createTaskThroughApi(
        context.request,
        app.origin,
        csrfToken,
        repositoryId,
        prompt,
      );
      const recoveryReached = await waitForReachedSignal(
        app.runtimeDir,
        recoveryReleasePath,
      );
      expect((await readTask(context.request, app.origin, created.id)).status).toBe("running");

      await publishReleaseSignal(bootstrapReached);
      await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();
      await expect(page.locator(".connection-banner")).toContainText("Store degraded");
      expect((await taskEvents(context.request, app.origin, created.id)).some(
        (event) => event.kind === "task.interrupted",
      )).toBe(false);
      await expect(page.locator(".connection-banner")).toContainText("Store degraded");

      await publishReleaseSignal(recoveryReached);
      const recovered = await waitForTask(
        context.request,
        app.origin,
        created.id,
        (task) => task.status === "interrupted",
        "service-generation recovery to persist interruption",
      );
      expect(recovered.failure?.code).toBe("STORE_WRITE_FAILED");
      await expect(page.locator(".connection-banner")).toContainText("Connected");

      await quitThroughUi(page, app);
    },
  );
});

test("store degradation stays visible until recovery persists the interrupted task", async (
  { context, page },
  testInfo,
) => {
  let recoveryReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      recoveryReleasePath = roots.releaseSignalPath("release-degraded-recovery");
      return {
        ...successScenario(),
        fake_scenarios: ["failure"],
        store_writer_faults: [
          {
            point: "fail_before_execute",
            operation: "finish_task",
            count: 1,
          },
          {
            point: "pause_before_execute",
            operation: "recover_incomplete",
            count: 1,
          },
        ],
        virtual_release_signals: [
          {
            name: "release-degraded-recovery",
            path: recoveryReleasePath,
            target: "store_writer_before_execute",
          },
        ],
      };
    },
    async (app) => {
      await openWorkspace(page, app);
      await addRepositoryThroughUi(page, app);
      const created = await createTaskThroughUi(
        page,
        app,
        "Recover an ambiguous terminal write",
      );
      if (recoveryReleasePath.length === 0) {
        throw new Error("degraded recovery release path was not configured");
      }
      const recoveryReached = await waitForReachedSignal(
        app.runtimeDir,
        recoveryReleasePath,
      );

      await expect(page.locator(".connection-banner")).toContainText("Store degraded", {
        timeout: API_TIMEOUT_MS,
      });
      const ambiguous = await readTask(context.request, app.origin, created.id);
      expect(ambiguous.status).toBe("running");
      expect((await taskEvents(context.request, app.origin, created.id)).some(
        (event) => event.kind === "task.interrupted",
      )).toBe(false);
      await expect(page.locator(".connection-banner")).toContainText("Store degraded");

      await publishReleaseSignal(recoveryReached);

      const recovered = await waitForTask(
        context.request,
        app.origin,
        created.id,
        (task) => task.status === "interrupted",
        "degraded recovery to interrupt the ambiguous task",
      );
      expect(recovered.failure?.code).toBe("STORE_WRITE_FAILED");
      const recoveryEvents = await taskEvents(context.request, app.origin, created.id);
      expect(recoveryEvents.filter((event) => event.kind === "task.interrupted")).toHaveLength(1);

      await expect(page.locator(".task-status-label")).toHaveText("Execution status: interrupted");
      await expect(
        page.locator(".failure-panel").getByText("STORE_WRITE_FAILED", { exact: true }),
      ).toBeVisible();
      await expect(page.locator(".connection-banner")).toContainText("Connected");
      const visibleAfterReady = await readTask(context.request, app.origin, created.id);
      expect(visibleAfterReady).toMatchObject({
        id: created.id,
        status: "interrupted",
        failure: { code: "STORE_WRITE_FAILED" },
      });

      await quitThroughUi(page, app);
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

async function addRepositoryThroughUi(page: Page, app: LocalApp): Promise<void> {
  await page.getByLabel("Repository path").fill(app.repositoryDir);
  const response = page.waitForResponse(
    (candidate) =>
      candidate.request().method() === "POST" &&
      candidate.url() === `${app.origin}/api/repositories`,
  );
  await page.getByRole("button", { name: "Add repository path" }).click();
  expect((await response).status()).toBe(201);
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

async function taskFromResponse(response: Response): Promise<TaskView> {
  expect(response.status()).toBe(201);
  return parseTask(await response.json());
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

async function taskEvents(
  api: APIRequestContext,
  origin: string,
  taskId: string,
): Promise<EventView[]> {
  const response = await api.get(
    `${origin}/api/tasks/${encodeURIComponent(taskId)}/events?after=0`,
  );
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

async function readCsrfToken(api: APIRequestContext, origin: string): Promise<string> {
  const response = await api.get(`${origin}/api/bootstrap`);
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!isRecord(candidate) || typeof candidate.csrf_token !== "string") {
    throw new Error("bootstrap response did not contain a CSRF token");
  }
  return candidate.csrf_token;
}

async function addRepositoryThroughApi(
  api: APIRequestContext,
  app: LocalApp,
  csrfToken: string,
): Promise<string> {
  const response = await api.post(`${app.origin}/api/repositories`, {
    headers: { origin: app.origin, "x-csrf-token": csrfToken },
    data: { path: app.repositoryDir },
  });
  expect(response.status()).toBe(201);
  const candidate = (await response.json()) as unknown;
  if (!isRecord(candidate) || typeof candidate.id !== "string") {
    throw new Error("repository response did not contain an ID");
  }
  return candidate.id;
}

async function createTaskThroughApi(
  api: APIRequestContext,
  origin: string,
  csrfToken: string,
  repositoryId: string,
  prompt: string,
): Promise<TaskView> {
  const response = await api.post(`${origin}/api/tasks`, {
    headers: { origin, "x-csrf-token": csrfToken },
    data: {
      client_request_id: randomUUID(),
      repository_id: repositoryId,
      prompt,
    },
  });
  expect(response.status()).toBe(201);
  return parseTask(await response.json());
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

async function readLiveFrame(
  page: Page,
  after: number,
  expectedEvent: string,
): Promise<CapturedSseFrame> {
  return page.evaluate(
    async ({ cursor, eventName, timeoutMs }) => {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const response = await fetch(`/api/events?after=${String(cursor)}`, {
          credentials: "same-origin",
          headers: { accept: "text/event-stream" },
          signal: controller.signal,
        });
        if (!response.ok || response.body === null) {
          throw new Error(`event stream failed with HTTP ${String(response.status)}`);
        }
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (true) {
          const result = await reader.read();
          buffer += decoder.decode(result.value, { stream: !result.done }).replace(/\r\n/gu, "\n");
          let separator = buffer.indexOf("\n\n");
          while (separator >= 0) {
            const raw = buffer.slice(0, separator);
            buffer = buffer.slice(separator + 2);
            const lines = raw.split("\n");
            const event = lines.find((line) => line.startsWith("event:"));
            const id = lines.find((line) => line.startsWith("id:"));
            const data = lines
              .filter((line) => line.startsWith("data:"))
              .map((line) => line.slice(5).trimStart())
              .join("\n");
            const eventValue = event?.slice(6).trimStart() ?? null;
            if (eventValue === eventName) {
              await reader.cancel();
              return {
                raw,
                event: eventValue,
                id: id?.slice(3).trimStart() ?? null,
                data: JSON.parse(data) as unknown,
              };
            }
            separator = buffer.indexOf("\n\n");
          }
          if (result.done) throw new Error(`event stream closed before ${eventName}`);
        }
      } finally {
        clearTimeout(timeout);
        controller.abort();
      }
    },
    { cursor: after, eventName: expectedEvent, timeoutMs: STREAM_TIMEOUT_MS },
  );
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
