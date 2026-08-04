import { expect, test, type Page } from "./fixtures";

import {
  publishUncoordinatedReleaseSignal,
  withLocalApp,
  type LocalApp,
} from "./support/localApp";
import {
  listStorageTasks,
  readStorageScheduler,
  storagePressureScenario,
  storageTask,
  type StorageReleasePaths,
  type StorageState,
  type StorageTask,
  type StorageTaskFailure,
  type StorageTaskStatus,
} from "./support/storageScheduler";

const FIRST_PROMPT = "keep running through storage admission blocks";
const SECOND_PROMPT = "wait until storage recovery";
const CRITICAL_FAILURE: StorageTaskFailure = {
  code: "DISK_PRESSURE_CRITICAL",
  message: "critical disk pressure stopped the task",
  retryable: true,
};

test("enforces storage admission, hysteresis, and critical stops in a real process", async (
  { page },
  testInfo,
) => {
  test.setTimeout(120_000);
  let releasePaths: StorageReleasePaths | null = null;

  await withLocalApp(
    testInfo,
    (roots) => {
      const setup = storagePressureScenario(roots);
      releasePaths = setup.releasePaths;
      return setup.scenario;
    },
    async (app) => {
      const releases = requireReleasePaths(releasePaths);
      await openWorkspace(page, app);
      await addRepositoryThroughUi(page, app);

      const firstTask = await createTaskThroughUi(page, app, FIRST_PROMPT);
      await expectTaskStatus(page, firstTask.id, "running");

      await releaseStorageSample(app, releases.pressure);
      await expectStorageState(page, "pressure");
      await expectTaskStatus(page, firstTask.id, "running");

      const secondTask = await createTaskThroughUi(page, app, SECOND_PROMPT);
      await expectTaskStatus(page, secondTask.id, "queued");
      await expectQueueReason(page, secondTask.id, "storage_pressure");
      await expect(
        page
          .getByLabel("Task workspace")
          .getByText("Waiting for storage", { exact: true }),
      ).toBeVisible();

      await releaseStorageSample(app, releases.unavailable);
      await expectStorageState(page, "unavailable");
      await expectTaskStatus(page, firstTask.id, "running");
      await expectTaskStatus(page, secondTask.id, "queued");

      await releaseStorageSample(app, releases.recovery);
      await page.waitForTimeout(2_000);
      expect((await readStorageScheduler(page)).storageState).toBe("unavailable");
      await expectTaskStatus(page, secondTask.id, "queued");
      await expectStorageState(page, "normal", 20_000);
      await expectTaskStatus(page, secondTask.id, "running");

      await releaseStorageSample(app, releases.critical);
      await expectStorageState(page, "critical");
      await expectTaskFailure(page, firstTask.id, CRITICAL_FAILURE);
      await expectTaskFailure(page, secondTask.id, CRITICAL_FAILURE);

      await quitThroughUi(page, app);
    },
  );
});

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
): Promise<StorageTask> {
  await page.getByLabel("Task description").fill(prompt);
  const responsePromise = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      response.url() === `${app.origin}/api/tasks`,
  );
  await page.getByRole("button", { name: "Create task", exact: true }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(201);
  return storageTask(await response.json());
}

async function expectTaskStatus(
  page: Page,
  taskId: string,
  status: StorageTaskStatus,
): Promise<void> {
  await expect
    .poll(async () => (await findTask(page, taskId)).status, {
      timeout: 20_000,
    })
    .toBe(status);
}

async function expectTaskFailure(
  page: Page,
  taskId: string,
  failure: StorageTaskFailure,
): Promise<void> {
  await expect
    .poll(
      async () => {
        const task = await findTask(page, taskId);
        return { status: task.status, failure: task.failure };
      },
      { timeout: 20_000 },
    )
    .toEqual({ status: "failed", failure });
}

async function expectStorageState(
  page: Page,
  state: StorageState,
  timeout = 15_000,
): Promise<void> {
  await expect
    .poll(async () => (await readStorageScheduler(page)).storageState, {
      timeout,
    })
    .toBe(state);
}

async function expectQueueReason(
  page: Page,
  taskId: string,
  reason: string,
): Promise<void> {
  await expect
    .poll(
      async () =>
        (await readStorageScheduler(page)).queuedTasks.find(
          (task) => task.taskId === taskId,
        )?.reason,
      { timeout: 20_000 },
    )
    .toBe(reason);
}

async function findTask(page: Page, taskId: string): Promise<StorageTask> {
  const task = (await listStorageTasks(page)).find(
    (candidate) => candidate.id === taskId,
  );
  if (task === undefined) {
    throw new Error(`task ${taskId} is missing from the task list`);
  }
  return task;
}

async function releaseStorageSample(
  app: LocalApp,
  releasePath: string,
): Promise<void> {
  await publishUncoordinatedReleaseSignal(app.runtimeDir, releasePath);
}

function requireReleasePaths(
  value: StorageReleasePaths | null,
): StorageReleasePaths {
  if (value === null) {
    throw new Error("storage release paths were not initialized");
  }
  return value;
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
