import {
  expect,
  test,
  type APIRequestContext,
  type Locator,
  type Page,
  type Response,
} from "./fixtures";

import {
  withLocalApp,
  type LocalApp,
  type ProcessScenario,
  type RepositorySnapshot,
} from "./support/localApp";

const API_TIMEOUT_MS = 15_000;
const CLEAN_EXIT_BUDGET_MS = 10_000;

interface RepositoryView {
  id: string;
  display_name: string;
  selected_path: string;
}

interface TaskView {
  id: string;
  prompt: string;
  repository_id: string;
  status: string;
}

test("adds two real repositories, reuses one identity, and switches the composer exactly", async (
  { context, page },
  testInfo,
) => {
  const scenario: ProcessScenario = {
    runtime_config: null,
    fake_scenarios: ["success", "success"],
    storage_samples: [{ kind: "native" }],
    store_writer_faults: [],
    actor_pauses: [],
    virtual_release_signals: [],
    legacy_v2_seed: { kind: "none" },
    marker_write_failure: false,
  };

  await withLocalApp(testInfo, scenario, async (app) => {
    const repositoryAPath = app.repositoryDir;
    const repositoryBPath = await app.createAdditionalRepository("fixture-repository-b");
    const before = await repositorySnapshots(app, repositoryAPath, repositoryBPath);

    await openWorkspace(page, app);

    const repositoryA = await addRepositoryThroughUi(page, app, repositoryAPath, 201);
    expect(repositoryA.display_name).toBe("fixture-repository");
    await expectSelectedRepository(page, repositoryA);

    const repositoryB = await addRepositoryThroughUi(page, app, repositoryBPath, 201);
    expect(repositoryB.display_name).toBe("fixture-repository-b");
    expect(repositoryB.id).not.toBe(repositoryA.id);
    await expectSelectedRepository(page, repositoryB);

    const reusedA = await addRepositoryThroughUi(page, app, repositoryAPath, 200);
    expect(reusedA.id).toBe(repositoryA.id);
    expect(reusedA.selected_path).toBe(repositoryA.selected_path);
    await expectSelectedRepository(page, repositoryA);

    const persistedRepositories = await listRepositories(context.request, app.origin);
    expect(persistedRepositories).toHaveLength(2);
    expect(new Set(persistedRepositories.map((repository) => repository.id))).toEqual(
      new Set([repositoryA.id, repositoryB.id]),
    );
    await expect(page.locator("button.repository-button")).toHaveCount(2);

    const promptA = "Composer targets repository A";
    const taskA = await createTaskThroughUi(page, app, promptA);
    expect(taskA.repository_id).toBe(repositoryA.id);
    await waitForTaskStatus(context.request, app.origin, taskA.id, "completed");

    await repositoryButton(page, repositoryB).click();
    await expectSelectedRepository(page, repositoryB);
    await expect(page.getByLabel("Task description")).toBeEnabled();
    await expect(page.getByRole("button", { name: `${promptA} Attempt 1`, exact: true }))
      .toHaveCount(0);

    const promptB = "Composer targets repository B";
    const taskB = await createTaskThroughUi(page, app, promptB);
    expect(taskB.repository_id).toBe(repositoryB.id);
    await waitForTaskStatus(context.request, app.origin, taskB.id, "completed");

    await repositoryButton(page, repositoryA).click();
    await expectSelectedRepository(page, repositoryA);
    await expect(page.getByRole("button", { name: `${promptA} Attempt 1`, exact: true }))
      .toBeVisible();
    await expect(page.getByRole("button", { name: `${promptB} Attempt 1`, exact: true }))
      .toHaveCount(0);

    await repositoryButton(page, repositoryB).click();
    await expectSelectedRepository(page, repositoryB);
    await expect(page.getByRole("button", { name: `${promptB} Attempt 1`, exact: true }))
      .toBeVisible();
    await expect(page.getByRole("button", { name: `${promptA} Attempt 1`, exact: true }))
      .toHaveCount(0);

    expect(await repositorySnapshots(app, repositoryAPath, repositoryBPath)).toEqual(before);
    await assertProcessIsConnected(page, app);
    await quitThroughUi(page, app);
  });
});

async function repositorySnapshots(
  app: LocalApp,
  repositoryAPath: string,
  repositoryBPath: string,
): Promise<{ repositoryA: RepositorySnapshot; repositoryB: RepositorySnapshot }> {
  const [repositoryA, repositoryB] = await Promise.all([
    app.repositorySnapshot(repositoryAPath),
    app.repositorySnapshot(repositoryBPath),
  ]);
  return { repositoryA, repositoryB };
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

async function addRepositoryThroughUi(
  page: Page,
  app: LocalApp,
  repositoryPath: string,
  expectedStatus: 200 | 201,
): Promise<RepositoryView> {
  await page.getByLabel("Repository path").fill(repositoryPath);
  const responsePromise = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      response.url() === `${app.origin}/api/repositories`,
  );
  await page.getByRole("button", { name: "Add repository path" }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(expectedStatus);
  return parseRepository(await response.json());
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
  return taskFromResponse(await responsePromise);
}

async function taskFromResponse(response: Response): Promise<TaskView> {
  expect(response.status()).toBe(201);
  return parseTask(await response.json());
}

async function listRepositories(
  api: APIRequestContext,
  origin: string,
): Promise<RepositoryView[]> {
  const response = await api.get(`${origin}/api/repositories`);
  expect(response.status()).toBe(200);
  const candidate = (await response.json()) as unknown;
  if (!Array.isArray(candidate)) throw new Error("repository list response was not an array");
  return candidate.map(parseRepository);
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

async function waitForTaskStatus(
  api: APIRequestContext,
  origin: string,
  taskId: string,
  status: string,
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

function repositoryButton(page: Page, repository: RepositoryView): Locator {
  return page.locator("button.repository-button").filter({
    has: page.getByText(repository.display_name, { exact: true }),
  });
}

async function expectSelectedRepository(page: Page, repository: RepositoryView): Promise<void> {
  const selected = repositoryButton(page, repository);
  await expect(selected).toHaveAttribute("aria-current", "page");
  await expect(page.locator('button.repository-button[aria-current="page"]')).toHaveCount(1);
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

function parseRepository(candidate: unknown): RepositoryView {
  if (
    !isRecord(candidate) ||
    typeof candidate.id !== "string" ||
    typeof candidate.display_name !== "string" ||
    typeof candidate.selected_path !== "string"
  ) {
    throw new Error("repository response contained invalid identity fields");
  }
  return {
    id: candidate.id,
    display_name: candidate.display_name,
    selected_path: candidate.selected_path,
  };
}

function parseTask(candidate: unknown): TaskView {
  if (
    !isRecord(candidate) ||
    typeof candidate.id !== "string" ||
    typeof candidate.prompt !== "string" ||
    typeof candidate.repository_id !== "string" ||
    typeof candidate.status !== "string"
  ) {
    throw new Error("task response contained invalid identity fields");
  }
  return {
    id: candidate.id,
    prompt: candidate.prompt,
    repository_id: candidate.repository_id,
    status: candidate.status,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
