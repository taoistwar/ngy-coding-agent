import { expect, type Locator, type Page, type Response } from "@playwright/test";

import type { LocalApp } from "./localApp";
import { DeliveryGit, type DeliveryTargetSnapshot, type ExactNoFfMerge } from "./delivery/git";
import { ExactAppOriginTrafficGuard } from "./delivery/network";
import { loseNextHttpReply } from "./delivery/recovery";
import {
  DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
  PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
} from "./delivery/timeouts";
import {
  fetchDelivery,
  type DeliveryMergeState,
  type DeliveryTask,
  waitForDelivery,
  waitForMergeState,
  waitForTaskDeliveryReadiness,
} from "./delivery/wire";

const DELIVERY_COMMAND_TIMEOUT_MS = 30_000;

export interface CreatedDeliveryTask {
  readonly id: string;
  readonly prompt: string;
}

export class DeliveryBrowserDriver {
  readonly git: DeliveryGit;

  private constructor(
    readonly page: Page,
    readonly app: LocalApp,
    git: DeliveryGit,
    private readonly trafficGuard: ExactAppOriginTrafficGuard,
  ) {
    this.git = git;
  }

  static async open(page: Page, app: LocalApp): Promise<DeliveryBrowserDriver> {
    const git = await DeliveryGit.open(app.repositoryDir, app.root);
    const trafficGuard = ExactAppOriginTrafficGuard.install(page, app.origin);
    await app.open(page);
    await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();
    await expect(page.locator(".connection-banner")).toContainText("Connected");
    return new DeliveryBrowserDriver(page, app, git, trafficGuard);
  }

  async reopen(): Promise<void> {
    this.trafficGuard.expectOrigin(this.app.origin);
    await this.app.reopen(this.page);
    await expect(this.page.locator(".connection-banner")).toContainText("Connected");
  }

  async registerFixtureRepository(): Promise<void> {
    await this.page.getByLabel("Repository path").fill(this.app.repositoryDir);
    const responsePromise = this.page.waitForResponse(
      (response) =>
        response.request().method() === "POST" &&
        response.url() === `${this.app.origin}/api/repositories`,
    );
    await this.page.getByRole("button", { name: "Add repository path" }).click();
    const response = await responsePromise;
    if (![200, 201].includes(response.status())) {
      throw new Error(`repository registration failed with HTTP ${String(response.status())}`);
    }
    await expect(this.page.getByRole("button", { name: /^fixture-repository /u })).toBeVisible();
  }

  async createApprovedTask(prompt: string): Promise<CreatedDeliveryTask> {
    await this.page.getByLabel("Task description").fill(prompt);
    const responsePromise = this.page.waitForResponse(
      (response) =>
        response.request().method() === "POST" &&
        response.url() === `${this.app.origin}/api/tasks`,
    );
    await this.page.getByRole("button", { name: "Create task", exact: true }).click();
    const response = await responsePromise;
    if (response.status() !== 201) {
      throw new Error(`delivery task creation failed with HTTP ${String(response.status())}`);
    }
    const task = parseCreatedTask(await response.json(), prompt);
    await waitForTaskDeliveryReadiness(
      this.page,
      task.id,
      "review_approved",
      PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
    );
    await waitForDelivery(
      this.page,
      task.id,
      (delivery) => delivery.eligibility === "eligible",
      "eligible delivery projection after task finalization",
      DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
    );
    await this.selectTask(task.prompt);
    await expect(this.panel()).toBeVisible();
    await expect(this.panel()).toContainText("Eligible for local delivery", {
      timeout: DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
    });
    return task;
  }

  async selectTask(prompt: string): Promise<void> {
    await this.page
      .locator(".task-list-item")
      .filter({ has: this.page.getByText(prompt, { exact: true }) })
      .locator("button.task-button")
      .click();
  }

  async delivery(taskId: string): Promise<DeliveryTask> {
    return fetchDelivery(this.page, taskId);
  }

  async runPreflight(taskId: string, before: DeliveryTargetSnapshot): Promise<DeliveryTask> {
    const dialog = await this.openPreflightDialog(taskId, before);

    const responsePromise = this.deliveryResponse(
      "POST",
      `/api/tasks/${taskId}/merge/preflight`,
      PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
    );
    const submit = dialog.getByRole("button", { name: "Run preflight", exact: true });
    await submit.click();
    const response = await responsePromise;
    if (![200, 201].includes(response.status())) {
      throw new Error(`preflight command failed with HTTP ${String(response.status())}`);
    }
    await expect(dialog.getByRole("status", { name: "Delivery command receipt" })).toContainText(
      "Durable receipt:",
    );
    await this.git.assertTargetUnchanged(before);
    const pending = await response.json() as { operation?: { operation_id?: unknown } };
    const operationId = pending.operation?.operation_id;
    if (typeof operationId !== "string") {
      throw new Error("preflight response omitted operation ID");
    }
    await dialog.getByRole("button", { name: "Close" }).click();
    return waitForDelivery(
      this.page,
      taskId,
      (delivery) =>
        delivery.latest_merge?.operation_id === operationId &&
        !["preflight_pending", "abort_pending"].includes(delivery.latest_merge.state),
      "terminal preflight result",
      PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
    );
  }

  async acceptMerge(taskId: string, before: DeliveryTargetSnapshot): Promise<ExactNoFfMerge> {
    const ready = await waitForMergeState(this.page, taskId, "preflight_ready");
    const operation = ready.latest_merge;
    if (operation?.preflight_source_commit === null || operation?.preflight_source_commit === undefined) {
      throw new Error("ready preflight omitted its exact source commit");
    }
    const panel = this.panel();
    await panel.getByRole("button", { name: "Review and confirm local merge" }).click();
    const dialog = this.page.getByRole("dialog", { name: "Confirm exact local merge" });
    await expect(dialog).toBeVisible();
    await expectExactField(dialog, "Operation ID", operation.operation_id);
    await expectExactField(dialog, "Confirmed operation version", String(operation.version));
    await expectExactField(dialog, "Target HEAD", before.head);
    await expect(dialog).toContainText("changed version or authority tuple makes this confirmation stale");

    const responsePromise = this.deliveryResponse(
      "POST",
      `/api/tasks/${taskId}/merge`,
      PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
    );
    const mergeButton = dialog.getByRole("button", { name: "Merge locally", exact: true });
    await mergeButton.click();
    const response = await responsePromise;
    if (![200, 202].includes(response.status())) {
      throw new Error(`merge command failed with HTTP ${String(response.status())}`);
    }
    await expect(dialog.getByRole("status", { name: "Delivery command receipt" })).toContainText(
      "Durable receipt:",
    );
    await dialog.getByRole("button", { name: "Cancel" }).click();
    const merged = await waitForMergeState(
      this.page,
      taskId,
      "merged",
      PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
    );
    const durableSourceCommit = merged.latest_merge?.source_commit;
    if (
      durableSourceCommit === null ||
      durableSourceCommit === undefined ||
      merged.source?.source_oid !== durableSourceCommit ||
      merged.disposition?.source_oid !== durableSourceCommit
    ) {
      throw new Error("merged delivery omitted one exact durable source commit identity");
    }
    return this.git.assertExactNoFfMerge(before, durableSourceCommit);
  }

  async waitForMergeState(taskId: string, expected: DeliveryMergeState): Promise<DeliveryTask> {
    return waitForMergeState(
      this.page,
      taskId,
      expected,
      PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
    );
  }

  async startPreflightWithoutWaiting(
    taskId: string,
    before: DeliveryTargetSnapshot,
  ): Promise<void> {
    const dialog = await this.openPreflightDialog(taskId, before);
    await dialog.getByRole("button", { name: "Run preflight", exact: true }).click();
    await expect(dialog.getByRole("button", { name: "Running preflight…" })).toBeDisabled();
  }

  async startMergeWithoutWaiting(): Promise<void> {
    await this.panel().getByRole("button", { name: "Review and confirm local merge" }).click();
    const dialog = this.page.getByRole("dialog", { name: "Confirm exact local merge" });
    await dialog.getByRole("button", { name: "Merge locally", exact: true }).click();
    await expect(dialog.getByRole("button", { name: "Accepting merge…" })).toBeDisabled();
  }

  async startRemoveWorktreeWithoutWaiting(): Promise<void> {
    await this.panel().getByRole("button", { name: "Remove worktree", exact: true }).click();
    const dialog = this.page.getByRole("dialog", { name: "Remove local worktree?" });
    await dialog.getByRole("button", { name: "Remove exact local worktree" }).click();
    await expect(dialog.getByRole("button", { name: "Removing worktree…" })).toBeDisabled();
  }

  async startDeleteBranchWithoutWaiting(): Promise<void> {
    await this.panel().getByRole("button", { name: "Delete source branch", exact: true }).click();
    const dialog = this.page.getByRole("dialog", { name: "Delete local source branch?" });
    await dialog.getByRole("button", { name: "Delete exact local branch" }).click();
    await expect(
      dialog.getByRole("button", { name: "Deleting source branch…" }),
    ).toBeDisabled();
  }

  async removeWorktree(taskId: string): Promise<DeliveryTask> {
    const panel = this.panel();
    await panel.getByRole("button", { name: "Remove worktree", exact: true }).click();
    const dialog = this.page.getByRole("dialog", { name: "Remove local worktree?" });
    await expect(dialog).toContainText("The source branch is retained");
    const responsePromise = this.deliveryResponse(
      "POST",
      `/api/tasks/${taskId}/cleanup/worktree`,
      PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
    );
    await dialog.getByRole("button", { name: "Remove exact local worktree" }).click();
    const response = await responsePromise;
    if (![200, 202].includes(response.status())) {
      throw new Error(`worktree cleanup failed with HTTP ${String(response.status())}`);
    }
    await expect(dialog.getByRole("status", { name: "Remove worktree receipt" })).toContainText(
      "Durable receipt:",
    );
    await dialog.getByRole("button", { name: "Cancel" }).click();
    return waitForDelivery(
      this.page,
      taskId,
      (delivery) => delivery.disposition?.worktree.state === "removed",
      "removed worktree disposition",
      PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
    );
  }

  async removeWorktreeAfterLostReply(taskId: string): Promise<DeliveryTask> {
    const panel = this.panel();
    await panel.getByRole("button", { name: "Remove worktree", exact: true }).click();
    const dialog = this.page.getByRole("dialog", { name: "Remove local worktree?" });
    const requestBodies: unknown[] = [];
    const url = `${this.app.origin}/api/tasks/${taskId}/cleanup/worktree`;
    const observeRequest = (request: { method(): string; url(): string; postDataJSON(): unknown }) => {
      if (request.method() === "POST" && request.url() === url) {
        requestBodies.push(request.postDataJSON());
      }
    };
    this.page.on("request", observeRequest);
    try {
      const lost = await loseNextHttpReply(
        this.page,
        url,
        async () => {
          await dialog
            .getByRole("button", { name: "Remove exact local worktree" })
            .click({ clickCount: 2 });
        },
        PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
      );
      if (![200, 202].includes(lost.status)) {
        throw new Error(`lost worktree reply carried HTTP ${String(lost.status)}`);
      }
      await expect(dialog.getByRole("alert")).toContainText("NETWORK_ERROR");
      const retryResponsePromise = this.deliveryResponse(
        "POST",
        `/api/tasks/${taskId}/cleanup/worktree`,
        PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
      );
      await dialog.getByRole("button", { name: "Retry worktree cleanup" }).click();
      await expect(
        dialog.getByRole("button", { name: "Removing worktree…" }),
      ).toBeDisabled();
      const retryResponse = await retryResponsePromise;
      if (retryResponse.status() !== 200) {
        throw new Error(
          `replayed worktree cleanup did not return existing receipt: HTTP ${String(retryResponse.status())}`,
        );
      }
      const retryBody = await retryResponse.json() as { receipt?: unknown };
      if (retryBody.receipt !== "existing") {
        throw new Error("replayed worktree cleanup did not return an existing receipt");
      }
      await expect(dialog.getByRole("status", { name: "Remove worktree receipt" })).toContainText(
        "Durable receipt: existing",
      );
      if (requestBodies.length !== 2 || JSON.stringify(requestBodies[0]) !== JSON.stringify(requestBodies[1])) {
        throw new Error("lost-reply cleanup did not replay one exact client request ID");
      }
      await dialog.getByRole("button", { name: "Cancel" }).click();
    } finally {
      this.page.off("request", observeRequest);
    }
    return waitForDelivery(
      this.page,
      taskId,
      (delivery) => delivery.disposition?.worktree.state === "removed",
      "removed worktree disposition after lost reply",
      PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
    );
  }

  async deleteSourceBranch(taskId: string): Promise<DeliveryTask> {
    const panel = this.panel();
    await panel.getByRole("button", { name: "Delete source branch", exact: true }).click();
    const dialog = this.page.getByRole("dialog", { name: "Delete local source branch?" });
    await expect(dialog).toContainText("never deletes a remote branch");
    const responsePromise = this.deliveryResponse(
      "POST",
      `/api/tasks/${taskId}/cleanup/branch`,
      PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
    );
    await dialog.getByRole("button", { name: "Delete exact local branch" }).click();
    const response = await responsePromise;
    if (![200, 202].includes(response.status())) {
      throw new Error(`branch cleanup failed with HTTP ${String(response.status())}`);
    }
    await expect(dialog.getByRole("status", { name: "Delete source branch receipt" })).toContainText(
      "Durable receipt:",
    );
    await dialog.getByRole("button", { name: "Cancel" }).click();
    return waitForDelivery(
      this.page,
      taskId,
      (delivery) => delivery.disposition?.branch.state === "deleted",
      "deleted branch disposition",
      PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
    );
  }

  async assertDeliveryPanelRedactsAbsolutePaths(): Promise<void> {
    const text = await this.panel().innerText();
    for (const secretPath of [this.app.root, this.app.repositoryDir, this.app.appDataDir]) {
      if (text.includes(secretPath)) {
        throw new Error("delivery panel exposed an absolute fixture path");
      }
    }
  }

  async quit(): Promise<void> {
    await this.page.getByRole("button", { name: "Quit local application" }).click();
    const dialog = this.page.getByRole("dialog", { name: "Quit local application?" });
    await expect(dialog).toBeVisible();
    await Promise.all([
      this.app.waitForCleanExit(),
      dialog.getByRole("button", { name: "Quit application" }).click(),
    ]);
    this.trafficGuard.assertNoEscapesAndDispose();
  }

  panel(): Locator {
    return this.page.locator(".delivery-panel");
  }

  private deliveryResponse(
    method: string,
    requestPath: string,
    timeout = DELIVERY_COMMAND_TIMEOUT_MS,
  ): Promise<Response> {
    return this.page.waitForResponse(
      (response) =>
        response.request().method() === method &&
        response.url() === `${this.app.origin}${requestPath}`,
      { timeout },
    );
  }

  private async openPreflightDialog(
    taskId: string,
    before: DeliveryTargetSnapshot,
  ): Promise<Locator> {
    const projection = await fetchDelivery(this.page, taskId);
    if (projection.target.available !== true || !("branch" in projection.target)) {
      throw new Error("delivery target is unavailable before preflight");
    }
    await this.panel().getByRole("button", { name: "Run delivery preflight" }).click();
    const dialog = this.page.getByRole("dialog", { name: "Confirm local merge preflight" });
    await expect(dialog).toBeVisible();
    await expectExactField(dialog, "Target branch", projection.target.branch);
    await expectExactField(dialog, "Target HEAD", before.head);
    await expectExactField(
      dialog,
      "Source state",
      projection.source?.state ?? "Not materialized",
    );
    await expect(dialog).toContainText("without modifying the target branch");
    return dialog;
  }
}

export { DeliveryGit } from "./delivery/git";
export type { DeliveryTargetSnapshot, ExactNoFfMerge } from "./delivery/git";
export { loseNextHttpReply, productionDeliveryScenario } from "./delivery/recovery";
export {
  DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
  PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
  productionDeliveryTestTimeout,
} from "./delivery/timeouts";
export type { ProductionDeliveryScenarioStage } from "./delivery/timeouts";
export {
  fetchDelivery,
  fetchDeliveryOperation,
  waitForDelivery,
  waitForMergeState,
  waitForTaskDeliveryReadiness,
} from "./delivery/wire";

async function expectExactField(dialog: Locator, label: string, value: string): Promise<void> {
  const term = dialog.locator(".delivery-exact-fields dt").filter({ hasText: label });
  await expect(term).toHaveText(label);
  const row = term.locator("..");
  await expect(row.locator("dd")).toHaveText(value);
}

function parseCreatedTask(value: unknown, expectedPrompt: string): CreatedDeliveryTask {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("task creation response is not an object");
  }
  const task = value as Record<string, unknown>;
  if (typeof task.id !== "string" || task.prompt !== expectedPrompt) {
    throw new Error("task creation response omitted the exact task identity");
  }
  return Object.freeze({ id: task.id, prompt: expectedPrompt });
}
