import {
  expect,
  test,
  type APIRequestContext,
  type Page,
  type Response,
} from "./fixtures";

import {
  publishReleaseSignal,
  waitForReachedSignal,
  withLocalApp,
  type ActorPausePoint,
  type LocalApp,
  type ProcessScenario,
  type VirtualReleaseTarget,
} from "./support/localApp";

const API_TIMEOUT_MS = 10_000;
const CLEAN_EXIT_BUDGET_MS = 10_000;

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

interface ClaimRace {
  pause: Extract<
    ActorPausePoint,
    "claim_permit_acquired" | "claim_handle_registered" | "claim_running_committed"
  >;
  target: Extract<
    VirtualReleaseTarget,
    | "actor_claim_permit_acquired"
    | "actor_claim_handle_registered"
    | "actor_claim_running_committed"
  >;
  statusAtPause: "queued" | "running";
}

const CLAIM_RACES: readonly ClaimRace[] = [
  {
    pause: "claim_permit_acquired",
    target: "actor_claim_permit_acquired",
    statusAtPause: "queued",
  },
  {
    pause: "claim_handle_registered",
    target: "actor_claim_handle_registered",
    statusAtPause: "queued",
  },
  {
    pause: "claim_running_committed",
    target: "actor_claim_running_committed",
    statusAtPause: "running",
  },
] as const;

for (const race of CLAIM_RACES) {
  test(`cancel queued behind ${race.pause} commits one legal terminal outcome`, async (
    { context, page },
    testInfo,
  ) => {
    let actorReleasePath = "";
    let cancelEnqueuedReleasePath = "";
    await withLocalApp(
      testInfo,
      (roots): ProcessScenario => {
        actorReleasePath = roots.releaseSignalPath(race.pause);
        cancelEnqueuedReleasePath = roots.releaseSignalPath("cancel-enqueued");
        return {
          fake_scenarios: ["blocking"],
          store_writer_faults: [],
          actor_pauses: [race.pause, "cancel_enqueued"],
          virtual_release_signals: [
            {
              name: race.pause,
              path: actorReleasePath,
              target: race.target,
            },
            {
              name: "cancel-enqueued",
              path: cancelEnqueuedReleasePath,
              target: "actor_cancel_enqueued",
            },
          ],
          legacy_v2_seed: { kind: "none" },
          marker_write_failure: false,
        };
      },
      async (app) => {
        await openWorkspace(page, app);
        await addRepositoryThroughUi(page, app);

        const prompt = `Cancel race at ${race.pause}`;
        const creation = await beginTaskCreationThroughUi(page, app, prompt);
        if (actorReleasePath.length === 0 || cancelEnqueuedReleasePath.length === 0) {
          throw new Error("claim/cancel actor release paths were not initialized");
        }
        const claimReached = await waitForReachedSignal(app.runtimeDir, actorReleasePath);
        const created = await waitForTaskByPrompt(context.request, app.origin, prompt);
        await page
          .getByRole("button", { name: `${prompt} Attempt 1`, exact: true })
          .click();

        const paused = await readTask(context.request, app.origin, created.id);
        expect(paused.status).toBe(race.statusAtPause);
        await expectSelectedStatus(page, race.statusAtPause);

        const cancelUrl = `${app.origin}/api/tasks/${created.id}/cancel`;
        const cancelResponse = page.waitForResponse(
          (response) =>
            response.request().method() === "POST" && response.url() === cancelUrl,
          { timeout: 0 },
        );
        void cancelResponse.catch(() => undefined);
        await page.getByRole("button", { name: "Cancel task", exact: true }).click();

        // The process marker is published only after mpsc::send succeeds, so
        // this proves the cancellation is in the actor queue, not merely that
        // the browser emitted an HTTP request.
        const cancelEnqueuedReached = await waitForReachedSignal(
          app.runtimeDir,
          cancelEnqueuedReleasePath,
        );
        await expect(page.getByRole("button", { name: "Cancelling", exact: true }))
          .toBeDisabled();

        await publishReleaseSignal(claimReached);
        const createResponse = await within(
          creation.response,
          "the paused task-create response after releasing the claim",
        );
        expect(createResponse.status()).toBe(201);
        const createdFromResponse = parseTask(await createResponse.json());
        expect(createdFromResponse).toEqual(
          expect.objectContaining({ id: created.id, prompt, status: "queued" }),
        );
        await waitForTaskStatus(context.request, app.origin, created.id, "cancelled");
        await publishReleaseSignal(cancelEnqueuedReached);

        const response = await within(cancelResponse, "the cancel response after releasing the actor");
        const accepted = await cancellationAccepted(response);
        expect(accepted).toEqual({
          cancellation_requested: true,
          task: expect.objectContaining({ id: created.id, status: "running" }),
        });

        const terminal = await waitForTaskStatus(
          context.request,
          app.origin,
          created.id,
          "cancelled",
        );
        expect(terminal).toEqual(expect.objectContaining({ id: created.id, status: "cancelled" }));
        await expectSelectedStatus(page, "cancelled");

        const tasks = await listTasks(context.request, app.origin);
        expect(tasks).toEqual([
          expect.objectContaining({ id: created.id, status: "cancelled" }),
        ]);
        expect(tasks.some((task) => task.status === "queued" || task.status === "running"))
          .toBe(false);

        const expectedTimeline = ["task.queued", "task.started", "task.cancelled"];
        expect(await taskEventKinds(context.request, app.origin, created.id))
          .toEqual(expectedTimeline);
        for (const kind of expectedTimeline) {
          await expect(
            page.locator(".timeline-panel").getByText(kind, { exact: true }),
          ).toHaveCount(1);
        }

        await assertProcessIsConnected(page, app);
        await quitThroughUi(page, app);
      },
    );
  });
}

test("completion committed at result_before_write wins over an already-sent UI cancel", async (
  { context, page },
  testInfo,
) => {
  let actorReleasePath = "";
  let cancelEnqueuedReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      actorReleasePath = roots.releaseSignalPath("result-before-write");
      cancelEnqueuedReleasePath = roots.releaseSignalPath("cancel-enqueued");
      return {
        fake_scenarios: ["success"],
        store_writer_faults: [],
        actor_pauses: ["result_before_write", "cancel_enqueued"],
        virtual_release_signals: [
          {
            name: "result-before-write",
            path: actorReleasePath,
            target: "actor_result_before_write",
          },
          {
            name: "cancel-enqueued",
            path: cancelEnqueuedReleasePath,
            target: "actor_cancel_enqueued",
          },
        ],
        legacy_v2_seed: { kind: "none" },
        marker_write_failure: false,
      };
    },
    async (app) => {
      await openWorkspace(page, app);
      await addRepositoryThroughUi(page, app);

      const created = await createTaskThroughUi(
        page,
        app,
        "Completion wins the first durable terminal commit",
      );
      if (actorReleasePath.length === 0 || cancelEnqueuedReleasePath.length === 0) {
        throw new Error("result/cancel actor release paths were not initialized");
      }
      const resultReached = await waitForReachedSignal(app.runtimeDir, actorReleasePath);
      expect((await readTask(context.request, app.origin, created.id)).status).toBe("running");
      await expectSelectedStatus(page, "running");

      const cancelUrl = `${app.origin}/api/tasks/${created.id}/cancel`;
      const cancelResponse = page.waitForResponse(
        (response) => response.request().method() === "POST" && response.url() === cancelUrl,
      );
      await page.getByRole("button", { name: "Cancel task", exact: true }).click();
      const cancelEnqueuedReached = await waitForReachedSignal(
        app.runtimeDir,
        cancelEnqueuedReleasePath,
      );
      await expect(page.getByRole("button", { name: "Cancelling", exact: true }))
        .toBeDisabled();

      await publishReleaseSignal(resultReached);
      await waitForTaskStatus(context.request, app.origin, created.id, "completed");
      await publishReleaseSignal(cancelEnqueuedReached);

      const response = await cancelResponse;
      expect(response.status()).toBe(409);
      const error = (await response.json()) as unknown;
      expect(error).toEqual({
        code: "TASK_NOT_CANCELLABLE",
        details: {},
        message: "the task cannot be cancelled in its current state",
        request_id: expect.any(String),
        retryable: false,
      });

      await waitForTaskStatus(context.request, app.origin, created.id, "completed");
      await expectSelectedStatus(page, "completed");
      const tasks = await listTasks(context.request, app.origin);
      expect(tasks).toEqual([
        expect.objectContaining({ id: created.id, status: "completed" }),
      ]);
      expect(tasks.some((task) => task.status === "queued" || task.status === "running"))
        .toBe(false);

      const eventKinds = await taskEventKinds(context.request, app.origin, created.id);
      expect(eventKinds.at(-1)).toBe("task.completed");
      expect(
        eventKinds.filter(
          (kind) =>
            kind === "task.completed" ||
            kind === "task.failed" ||
            kind === "task.cancelled" ||
            kind === "task.interrupted",
        ),
      ).toEqual(["task.completed"]);
      await expect(
        page.locator(".timeline-panel").getByText("task.completed", { exact: true }),
      ).toHaveCount(1);
      await expect(
        page.locator(".timeline-panel").getByText("task.cancelled", { exact: true }),
      ).toHaveCount(0);

      await assertProcessIsConnected(page, app);
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
  const responsePromise = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" && response.url() === `${app.origin}/api/tasks`,
  );
  await page.getByRole("button", { name: "Create task", exact: true }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(201);
  const task = parseTask(await response.json());
  expect(task.prompt).toBe(prompt);
  return task;
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

async function cancellationAccepted(response: Response): Promise<{
  cancellation_requested: true;
  task: TaskView;
}> {
  expect(response.status()).toBe(202);
  const candidate = (await response.json()) as unknown;
  if (
    !isRecord(candidate) ||
    candidate.cancellation_requested !== true ||
    !hasExactKeys(candidate, ["cancellation_requested", "task"])
  ) {
    throw new Error("cancel response was not the exact accepted shape");
  }
  return {
    cancellation_requested: true,
    task: parseTask(candidate.task),
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
