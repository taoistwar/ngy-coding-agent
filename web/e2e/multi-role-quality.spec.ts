import { expect, test, type APIRequestContext, type Page, type Response } from "./fixtures";

import {
  withLocalApp,
  type FakeScenario,
  type LocalApp,
  type ProcessScenario,
} from "./support/localApp";

const API_TIMEOUT_MS = 15_000;
const CLEAN_EXIT_BUDGET_MS = 10_000;
const STREAM_TIMEOUT_MS = 10_000;

type TaskStatus = "completed" | "failed";
type DeliveryReadiness = "unreviewed" | "review_approved" | "review_rejected";

interface TaskWire {
  id: string;
  prompt: string;
  status: string;
  delivery_readiness: DeliveryReadiness;
  failure: { code: string } | null;
}

interface TaskDetailWire {
  task: TaskWire;
  reviews: Array<{
    round: number;
    verdict: "approved" | "changes_requested";
    workspace_generation: number;
  }>;
}

interface CapturedSseFrame {
  event: string;
  data: unknown;
  observedEvents: string[];
}

test("shows a first-round approval as reviewed evidence without merge controls", async (
  { context, page },
  testInfo,
) => {
  await withLocalApp(
    testInfo,
    processScenario("multi_role_approved"),
    async (app) => {
      await openWorkspace(page, app);
      await addRepositoryThroughUi(page, app);
      const task = await createTaskThroughUi(
        page,
        app,
        "Approve the deterministic change in the first review round",
      );
      await waitForTask(
        context.request,
        app.origin,
        task.id,
        "completed",
        "review_approved",
      );

      await expectDelivery(page, "completed", "review approved");
      await expect(page.getByTestId("review-round-1")).toContainText("Approved");
      await expect(page.getByTestId("review-round-1")).toContainText(
        "Final delivery review",
      );
      await expect(page.getByTestId("review-round-1")).toContainText(
        "Workspace generation",
      );
      await expect(
        page.getByTestId("review-round-1").locator(".review-meta dd").nth(1),
      ).toHaveText("1");
      await expectRoleActivity(page, ["Planner #1", "Executor #1", "Reviewer #1"]);
      await expectCurrentGeneration(page, 1);
      await expectNoReviewGenerationMismatchWarning(page);
      await expectEvidencePanelOrder(page);
      await expectNoMergeOrApprovalControls(page);
      await quitThroughUi(page, app);
    },
  );
});

test("keeps the rejected first round historical after one rework is approved", async (
  { context, page },
  testInfo,
) => {
  await withLocalApp(
    testInfo,
    processScenario("multi_role_rework_approved"),
    async (app) => {
      await openWorkspace(page, app);
      await addRepositoryThroughUi(page, app);
      const task = await createTaskThroughUi(
        page,
        app,
        "Rework one bounded finding and approve the second generation",
      );
      await waitForTask(
        context.request,
        app.origin,
        task.id,
        "completed",
        "review_approved",
      );

      await expectDelivery(page, "completed", "review approved");
      const first = page.getByTestId("review-round-1");
      const second = page.getByTestId("review-round-2");
      await expect(first).toContainText("Changes requested");
      await expect(first).toContainText("Historical");
      await expect(first).toContainText(
        "Reviewer round 1 requests one bounded correction",
      );
      await expect(second).toContainText("Approved");
      await expect(second).toContainText("Final delivery review");
      await expectRoleActivity(page, [
        "Planner #1",
        "Executor #1",
        "Reviewer #1",
        "Executor #2",
        "Reviewer #2",
      ]);
      await expectCurrentGeneration(page, 2);
      await expectNoReviewGenerationMismatchWarning(page);
      await expectEvidencePanelOrder(page);
      await expectNoMergeOrApprovalControls(page);
      await quitThroughUi(page, app);
    },
  );
});

test("shows the third changes-requested verdict as final review rejection", async (
  { context, page },
  testInfo,
) => {
  await withLocalApp(
    testInfo,
    processScenario("multi_role_rejected"),
    async (app) => {
      await openWorkspace(page, app);
      await addRepositoryThroughUi(page, app);
      const task = await createTaskThroughUi(
        page,
        app,
        "Reject the deterministic change after all bounded review rounds",
      );
      const terminal = await waitForTask(
        context.request,
        app.origin,
        task.id,
        "failed",
        "review_rejected",
      );
      expect(terminal.failure?.code).toBe("REVIEW_REJECTED");

      await expectDelivery(page, "failed", "review rejected");
      await expect(page.locator(".failure-panel")).toContainText("REVIEW_REJECTED");
      await expect(page.getByTestId("review-round-1")).toContainText("Historical");
      await expect(page.getByTestId("review-round-2")).toContainText("Historical");
      const finalReview = page.getByTestId("review-round-3");
      await expect(finalReview).toContainText("Changes requested");
      await expect(finalReview).toContainText("Final delivery review");
      await expect(finalReview).toContainText(
        "Reviewer round 3 requests one bounded correction",
      );
      await expectRoleActivity(page, [
        "Planner #1",
        "Executor #1",
        "Reviewer #1",
        "Executor #2",
        "Reviewer #2",
        "Executor #3",
        "Reviewer #3",
      ]);
      await expectCurrentGeneration(page, 3);
      await expectEvidencePanelOrder(page);
      await expectNoMergeOrApprovalControls(page);
      await quitThroughUi(page, app);
    },
  );
});

test("migrates a real v2 Completed task as unreviewed in REST, SSE, and React", async (
  { context, page },
  testInfo,
) => {
  const prompt = "Legacy v2 Completed task remains explicitly unreviewed";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => ({
      runtime_config: null,
      fake_scenarios: [],
      storage_samples: [{ kind: "native" }],
      store_writer_faults: [],
      actor_pauses: [],
      virtual_release_signals: [],
      legacy_v2_seed: {
        kind: "completed_task",
        repository_path: roots.repositoryDir,
        task_prompt: prompt,
      },
      marker_write_failure: false,
    }),
    async (app) => {
      await openWorkspace(page, app);
      const tasks = await listTasks(context.request, app.origin);
      expect(tasks).toHaveLength(1);
      const task = tasks[0];
      if (task === undefined) throw new Error("legacy v2 task was not projected");
      expect(task).toMatchObject({
        prompt,
        status: "completed",
        delivery_readiness: "unreviewed",
      });

      const detail = await readTaskDetail(context.request, app.origin, task.id);
      expect(detail.task.delivery_readiness).toBe("unreviewed");
      expect(detail.reviews).toEqual([]);

      const frame = await readSseFrame(page, 0, "task.completed");
      expect(frame.observedEvents).not.toContain("review.updated");
      expect(frame.data).toMatchObject({
        kind: "task.completed",
        payload: {
          task: {
            id: task.id,
            status: "completed",
            delivery_readiness: "unreviewed",
          },
        },
      });

      await selectTask(page, prompt);
      await expectDelivery(page, "completed", "unreviewed");
      await expect(page.getByText("Execution completed — not reviewed")).toBeVisible();
      await expect(page.locator(".review-panel")).toContainText(
        "No review evidence is available yet.",
      );
      await expect(page.locator(".legacy-plan-note")).toContainText(
        "Legacy plan",
      );
      await expect(page.locator(".task-list-readiness")).toHaveText("Unreviewed");
      await expectEvidencePanelOrder(page);
      await expectNoMergeOrApprovalControls(page);
      await quitThroughUi(page, app);
    },
  );
});

function processScenario(fakeScenario: FakeScenario): ProcessScenario {
  return {
    runtime_config: null,
    fake_scenarios: [fakeScenario],
    storage_samples: [{ kind: "native" }],
    store_writer_faults: [],
    actor_pauses: [],
    virtual_release_signals: [],
    legacy_v2_seed: { kind: "none" },
    marker_write_failure: false,
  };
}

async function openWorkspace(page: Page, app: LocalApp): Promise<void> {
  const bootstrap = page.waitForResponse(
    (response) =>
      response.url() === `${app.origin}/api/bootstrap` && response.status() === 200,
  );
  await app.open(page);
  await bootstrap;
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
  await response.body();
}

async function createTaskThroughUi(
  page: Page,
  app: LocalApp,
  prompt: string,
): Promise<TaskWire> {
  await page.getByLabel("Task description").fill(prompt);
  const responsePromise: Promise<Response> = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      response.url() === `${app.origin}/api/tasks`,
  );
  await page.getByRole("button", { name: "Create task", exact: true }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(201);
  return parseTask(await response.json());
}

async function waitForTask(
  api: APIRequestContext,
  origin: string,
  taskId: string,
  status: TaskStatus,
  readiness: DeliveryReadiness,
): Promise<TaskWire> {
  const deadline = Date.now() + API_TIMEOUT_MS;
  while (true) {
    const detail = await readTaskDetail(api, origin, taskId);
    if (
      detail.task.status === status &&
      detail.task.delivery_readiness === readiness
    ) {
      return detail.task;
    }
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      throw new Error(
        `timed out waiting for ${status} + ${readiness}; last=${detail.task.status} + ${detail.task.delivery_readiness}`,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, Math.min(25, remaining)));
  }
}

async function readTaskDetail(
  api: APIRequestContext,
  origin: string,
  taskId: string,
): Promise<TaskDetailWire> {
  const response = await api.get(
    `${origin}/api/tasks/${encodeURIComponent(taskId)}`,
  );
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!isRecord(candidate) || !Array.isArray(candidate.reviews)) {
    throw new Error("task detail response was invalid");
  }
  return {
    task: parseTask(candidate.task),
    reviews: candidate.reviews.map((review) => {
      if (
        !isRecord(review) ||
        typeof review.round !== "number" ||
        (review.verdict !== "approved" &&
          review.verdict !== "changes_requested") ||
        typeof review.workspace_generation !== "number"
      ) {
        throw new Error("task detail contained invalid review evidence");
      }
      return {
        round: review.round,
        verdict: review.verdict,
        workspace_generation: review.workspace_generation,
      };
    }),
  };
}

async function listTasks(
  api: APIRequestContext,
  origin: string,
): Promise<TaskWire[]> {
  const response = await api.get(`${origin}/api/tasks`);
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!Array.isArray(candidate)) throw new Error("task list response was invalid");
  return candidate.map(parseTask);
}

function parseTask(candidate: unknown): TaskWire {
  if (
    !isRecord(candidate) ||
    typeof candidate.id !== "string" ||
    typeof candidate.prompt !== "string" ||
    typeof candidate.status !== "string" ||
    !isDeliveryReadiness(candidate.delivery_readiness) ||
    !(
      candidate.failure === null ||
      (isRecord(candidate.failure) && typeof candidate.failure.code === "string")
    )
  ) {
    throw new Error("task response was invalid");
  }
  return {
    id: candidate.id,
    prompt: candidate.prompt,
    status: candidate.status,
    delivery_readiness: candidate.delivery_readiness,
    failure:
      candidate.failure === null ? null : { code: candidate.failure.code as string },
  };
}

function isDeliveryReadiness(value: unknown): value is DeliveryReadiness {
  return (
    value === "unreviewed" ||
    value === "review_approved" ||
    value === "review_rejected"
  );
}

async function selectTask(page: Page, prompt: string): Promise<void> {
  await page.getByRole("button", { name: `${prompt} Attempt 1`, exact: true }).click();
}

async function expectDelivery(
  page: Page,
  status: TaskStatus,
  readiness: "unreviewed" | "review approved" | "review rejected",
): Promise<void> {
  await expect(page.locator(".task-status-label")).toHaveText(
    `Execution status: ${status}`,
  );
  await expect(page.locator(".task-readiness")).toHaveText(
    `Delivery readiness: ${readiness}`,
  );
  const sidebarLabel =
    readiness === "unreviewed"
      ? "Unreviewed"
      : readiness === "review approved"
        ? "Review approved"
        : "Review rejected";
  await expect(page.locator(".task-list-readiness")).toHaveText(sidebarLabel);
}

async function expectRoleActivity(
  page: Page,
  roles: readonly string[],
): Promise<void> {
  for (const role of roles) {
    await expect(
      page.locator(".activity-panel").getByText(role, { exact: true }),
    ).toBeVisible();
  }
}

async function expectCurrentGeneration(
  page: Page,
  generation: number,
): Promise<void> {
  await expect(page.locator(".diff-panel")).toContainText(
    `Workspace generation ${generation}`,
  );
  await expect(page.locator(".tests-panel")).toContainText(
    `Workspace generation ${generation}`,
  );
}

async function expectNoReviewGenerationMismatchWarning(
  page: Page,
): Promise<void> {
  await expect(
    page.locator(".review-panel .generation-warning").filter({
      hasText: "Generation mismatch:",
    }),
  ).toHaveCount(0);
}

async function expectEvidencePanelOrder(page: Page): Promise<void> {
  const panelClasses = await page
    .locator(".result-pane > .evidence-panel")
    .evaluateAll((panels) => panels.map((panel) => panel.className));
  const review = panelClasses.findIndex((value) => value.includes("review-panel"));
  const diff = panelClasses.findIndex((value) => value.includes("diff-panel"));
  const tests = panelClasses.findIndex((value) => value.includes("tests-panel"));
  const timeline = panelClasses.findIndex((value) => value.includes("timeline-panel"));
  expect(review).toBeGreaterThanOrEqual(0);
  expect(diff).toBeGreaterThan(review);
  expect(tests).toBeGreaterThan(diff);
  expect(timeline).toBeGreaterThan(tests);
}

async function expectNoMergeOrApprovalControls(page: Page): Promise<void> {
  await expect(
    page.getByRole("button", {
      name: /^(?:merge|merge changes|approve|approve changes)$/iu,
    }),
  ).toHaveCount(0);
}

async function readSseFrame(
  page: Page,
  after: number,
  expectedEvent: string,
): Promise<CapturedSseFrame> {
  return page.evaluate(
    async ({ cursor, eventName, timeoutMs }) => {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), timeoutMs);
      const observedEvents: string[] = [];
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
          buffer += decoder
            .decode(result.value, { stream: !result.done })
            .replace(/\r\n/gu, "\n");
          let separator = buffer.indexOf("\n\n");
          while (separator >= 0) {
            const raw = buffer.slice(0, separator);
            buffer = buffer.slice(separator + 2);
            const lines = raw.split("\n");
            const event = lines
              .find((line) => line.startsWith("event:"))
              ?.slice(6)
              .trimStart();
            const data = lines
              .filter((line) => line.startsWith("data:"))
              .map((line) => line.slice(5).trimStart())
              .join("\n");
            if (event !== undefined) observedEvents.push(event);
            if (event === eventName) {
              await reader.cancel();
              return {
                event,
                data: JSON.parse(data) as unknown,
                observedEvents,
              };
            }
            separator = buffer.indexOf("\n\n");
          }
          if (result.done) {
            throw new Error(`event stream closed before ${eventName}`);
          }
        }
      } finally {
        clearTimeout(timeout);
        controller.abort();
      }
    },
    { cursor: after, eventName: expectedEvent, timeoutMs: STREAM_TIMEOUT_MS },
  );
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
