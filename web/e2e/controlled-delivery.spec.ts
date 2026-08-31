import { access, writeFile } from "node:fs/promises";
import path from "node:path";

import { expect, test } from "./fixtures";

import {
  DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
  DeliveryBrowserDriver,
  fetchDelivery,
  PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
  productionDeliveryScenario,
  productionDeliveryTestTimeout,
} from "./support/delivery";
import {
  publishReleaseSignal,
  waitForReachedSignal,
  withLocalApp,
  type ProcessScenario,
} from "./support/localApp";

test("explicitly preflights and exact-no-ff merges real approved bytes, then cleans up separately", async (
  { page },
  testInfo,
) => {
  test.setTimeout(productionDeliveryTestTimeout(
    "approval",
    "preflight",
    "merge",
    "worktree_cleanup",
    "branch_cleanup",
  ));
  let terminalReleasePath = "";
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario => {
      terminalReleasePath = roots.releaseSignalPath("delivery-terminal-release");
      return productionDeliveryScenario(roots, {
        providerScenario: "approve",
        pauseAfterCommit: {
          name: "delivery-preflight-pending",
          operation: "create_merge_preflight",
        },
        pauseAfterTerminalDispatch: { name: "delivery-terminal-release" },
      });
    },
    async (app) => {
      const driver = await DeliveryBrowserDriver.open(page, app);
      await driver.registerFixtureRepository();
      const task = await driver.createReviewApprovedTask(
        "Approve exact local delivery bytes without publishing or deploying",
      );
      if (terminalReleasePath.length === 0) {
        throw new Error("terminal actor release path was not initialized");
      }
      const terminalReached = await waitForReachedSignal(
        app.runtimeDir,
        terminalReleasePath,
        PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
      );
      try {
        await expect(page.getByText("Execution status: completed", { exact: true })).toBeVisible({
          timeout: DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
        });
        await expect(
          page.getByText("Delivery readiness: review approved", { exact: true }),
        ).toBeVisible({ timeout: DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS });
        await expect(driver.panel()).toContainText("The task is still active.", {
          timeout: DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
        });
        await expect(driver.panel()).not.toContainText("Eligible for local delivery");
      } finally {
        await publishReleaseSignal(terminalReached);
      }
      await driver.waitForEligibleDelivery(task);
      const targetBefore = await driver.git.snapshotTarget();
      const sourceRefsBefore = await driver.git.sourceRefs();
      const beforeClick = await fetchDelivery(page, task.id);
      expect(beforeClick.source).toBeNull();
      expect(beforeClick.latest_merge).toBeNull();
      expect(beforeClick.disposition).toBeNull();
      await driver.git.assertTargetUnchanged(targetBefore);
      expect(await driver.git.sourceRefs()).toEqual(sourceRefsBefore);
      await driver.assertDeliveryPanelRedactsAbsolutePaths();

      await driver.startPreflightWithoutWaiting(task.id, targetBefore);
      const preflightRelease = path.join(
        app.runtimeDir,
        "signals",
        "delivery-preflight-pending.release",
      );
      const reached = await waitForReachedSignal(
        app.runtimeDir,
        preflightRelease,
        PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
      );
      try {
        await driver.waitForMergeState(task.id, "preflight_pending");
        await expect(driver.panel()).toContainText("Running preflight", {
          timeout: DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
        });
      } finally {
        await publishReleaseSignal(reached);
      }
      const ready = await driver.waitForMergeState(task.id, "preflight_ready");
      expect(ready.latest_merge?.state).toBe("preflight_ready");
      await driver.git.assertTargetUnchanged(targetBefore);

      await page.reload();
      await expect(page.locator(".connection-banner")).toContainText("Connected");
      await driver.selectTask(task.prompt);
      await expect(driver.panel()).toContainText("Preflight ready", {
        timeout: DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
      });
      const exactMerge = await driver.acceptMerge(task.id, targetBefore);

      const merged = await fetchDelivery(page, task.id);
      expect(merged.latest_merge?.state).toBe("merged");
      expect(merged.disposition?.worktree.state).toBe("retained_locked");
      expect(merged.disposition?.branch.state).toBe("retained");
      const sourceRef = merged.disposition?.source_ref;
      const sourceOid = merged.disposition?.source_oid;
      if (sourceRef === undefined || sourceOid === undefined) {
        throw new Error("merged projection omitted retained source identity");
      }
      expect(exactMerge.secondParent).toBe(sourceOid);
      expect(merged.latest_merge?.source_commit).toBe(sourceOid);
      expect(merged.source?.source_oid).toBe(sourceOid);
      expect(exactMerge.secondParent).not.toBe(merged.latest_merge?.preflight_source_commit);
      expect(await driver.git.refOid(sourceRef)).toBe(sourceOid);

      await driver.git.removeFixtureCargoTarget(sourceRef, sourceOid);

      const removed = await driver.removeWorktreeAfterLostReply(task.id);
      expect(removed.disposition?.worktree.state).toBe("removed");
      expect(removed.disposition?.branch.state).toBe("retained");
      expect(await driver.git.refOid(sourceRef)).toBe(sourceOid);
      await expect(driver.panel()).toContainText("Remove worktree receipt:");

      const deleted = await driver.deleteSourceBranch(task.id);
      expect(deleted.disposition?.worktree.state).toBe("removed");
      expect(deleted.disposition?.branch.state).toBe("deleted");
      expect(await driver.git.refOid(sourceRef)).toBeNull();
      await expect(driver.panel()).toContainText("Delete source branch receipt:");
      await driver.quit();
    },
  );
});

test("keeps conflicting and stale preflights byte-exact until a fresh explicit merge", async (
  { page },
  testInfo,
) => {
  test.setTimeout(productionDeliveryTestTimeout(
    "approval",
    "preflight",
    "preflight",
    "merge",
    "preflight",
    "merge",
  ));
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario =>
      productionDeliveryScenario(roots, { providerScenario: "conflict" }),
    async (app) => {
      const driver = await DeliveryBrowserDriver.open(page, app);
      await driver.registerFixtureRepository();
      const task = await driver.createApprovedTask(
        "Prove conflicts and stale authority never mutate the local target",
      );

      await driver.git.writeFixtureFile(
        "src/lib.rs",
        "pub fn fixture_value() -> u32 { 45 }\n// conflicting target advance\n",
      );
      await driver.git.commitAll("create explicit delivery conflict on target");
      const conflictingTarget = await driver.git.snapshotTarget();
      await page.reload();
      await expect(page.locator(".connection-banner")).toContainText("Connected");
      await driver.selectTask(task.prompt);
      const conflict = await driver.runPreflight(task.id, conflictingTarget);
      expect(conflict.latest_merge?.state).toBe("conflict");
      await driver.git.assertTargetUnchanged(conflictingTarget);
      await expect(driver.panel()).toContainText("Preflight found conflicts", {
        timeout: DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
      });
      await expect(driver.panel()).toContainText("src/lib.rs");
      await driver.assertDeliveryPanelRedactsAbsolutePaths();

      await driver.git.writeFixtureFile(
        "src/lib.rs",
        "pub fn fixture_value() -> u32 { 42 }\n",
      );
      await driver.git.commitAll("resolve delivery conflict outside the application");
      const resolvedTarget = await driver.git.snapshotTarget();
      await page.reload();
      await expect(page.locator(".connection-banner")).toContainText("Connected");
      await driver.selectTask(task.prompt);
      const ready = await driver.runPreflight(task.id, resolvedTarget);
      expect(ready.latest_merge?.state).toBe("preflight_ready");

      const staleDialog = await driver.openMergeDialog();
      await driver.git.writeFixtureFile(
        "target-only-note.txt",
        "independent target advance after the reviewed preflight\n",
      );
      await driver.git.commitAll("advance target after ready preflight");
      const advancedTarget = await driver.git.snapshotTarget();
      const recoveryDocumentMarker = `delivery-recovery-${Date.now()}`;
      await page.evaluate((marker) => {
        document.documentElement.dataset.deliveryRecoveryDocument = marker;
      }, recoveryDocumentMarker);
      const staleResponsePromise = page.waitForResponse(
        (response) =>
          response.request().method() === "POST" &&
          response.url() === `${app.origin}/api/tasks/${task.id}/merge`,
        { timeout: PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS },
      );
      await staleDialog
        .getByRole("button", { name: "Merge locally", exact: true })
        .click();
      const staleResponse = await staleResponsePromise;
      expect(staleResponse.status()).toBe(409);
      const staleError = (await staleResponse.json()) as { code?: unknown };
      expect(staleError.code).toBe("TARGET_HEAD_CHANGED");
      await expect(
        staleDialog.getByText("TARGET_HEAD_CHANGED", { exact: true }),
      ).toBeVisible();
      await driver.git.assertTargetUnchanged(advancedTarget);
      await driver.waitForMergeState(task.id, "stale");
      await expect(driver.panel()).toContainText("Preflight stale", {
        timeout: DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
      });
      await expect(
        driver
          .panel()
          .getByRole("button", { name: "Review and confirm local merge" }),
      ).toHaveCount(0);
      expect(
        await page.evaluate(
          () => document.documentElement.dataset.deliveryRecoveryDocument,
        ),
      ).toBe(recoveryDocumentMarker);
      await staleDialog.getByRole("button", { name: "Cancel" }).click();
      await expect(
        driver.panel().getByRole("button", { name: "Run delivery preflight" }),
      ).toBeEnabled({ timeout: DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS });

      const freshReady = await driver.runPreflight(task.id, advancedTarget);
      expect(freshReady.latest_merge?.state).toBe("preflight_ready");
      await driver.acceptMerge(task.id, advancedTarget);
      await driver.quit();
    },
  );
});

test("rejects an ignored target collision without overwriting its bytes", async (
  { page },
  testInfo,
) => {
  test.setTimeout(productionDeliveryTestTimeout("approval", "preflight"));
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario =>
      productionDeliveryScenario(roots, { providerScenario: "ignored_collision" }),
    async (app) => {
      const driver = await DeliveryBrowserDriver.open(page, app);
      await driver.registerFixtureRepository();
      const task = await driver.createApprovedTask(
        "Reject an ignored target collision without touching the user sentinel",
      );
      await driver.git.writeFixtureFile(".gitignore", "collision.txt\n");
      await driver.git.commitAll("ignore the delivery collision fixture");
      const sentinel = Buffer.from("ignored target sentinel must survive\n", "utf8");
      await driver.git.writeFixtureFile("collision.txt", sentinel.toString("utf8"));
      const targetBefore = await driver.git.snapshotTarget();

      await page.reload();
      await expect(page.locator(".connection-banner")).toContainText("Connected");
      await driver.selectTask(task.prompt);
      const rejected = await driver.runPreflight(task.id, targetBefore);
      expect(rejected.latest_merge?.state).toBe("rejected");
      await driver.git.assertTargetUnchanged(targetBefore);
      expect(await driver.git.readFixtureFile("collision.txt")).toEqual(sentinel);
      await expect(driver.panel()).toContainText("TARGET_IGNORED_PATH_COLLISION", {
        timeout: DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
      });
      await driver.assertDeliveryPanelRedactsAbsolutePaths();
      await driver.quit();
    },
  );
});

test("rejects malicious local Git configuration without executing or exposing it", async (
  { page },
  testInfo,
) => {
  test.setTimeout(productionDeliveryTestTimeout("approval"));
  await withLocalApp(
    testInfo,
    (roots): ProcessScenario =>
      productionDeliveryScenario(roots, { providerScenario: "approve" }),
    async (app) => {
      const driver = await DeliveryBrowserDriver.open(page, app);
      await driver.registerFixtureRepository();
      const task = await driver.createApprovedTask(
        "Reject unsafe repository configuration without invoking repository code",
      );
      const targetBefore = await driver.git.snapshotTarget();
      const sentinel = path.join(app.root, "malicious-config-helper-ran");
      const helperScript = path.join(app.root, "malicious-filter-helper.mjs");
      await writeFile(
        helperScript,
        [
          'import { writeFileSync } from "node:fs";',
          `writeFileSync(${JSON.stringify(sentinel)}, "executed\\n", "utf8");`,
          "",
        ].join("\n"),
        "utf8",
      );
      const helper = [process.execPath, helperScript].map(quotedGitHelperPath).join(" ");
      await driver.git.setLocalConfig("filter.delivery-probe.process", helper);

      await page.reload();
      await expect(page.locator(".connection-banner")).toContainText("Connected");
      await driver.selectTask(task.prompt);
      const rejected = await driver.delivery(task.id);
      expect(rejected.eligibility).not.toBe("eligible");
      expect(rejected.reasons).toContain("unsafe_git_configuration");
      await expect(driver.panel()).toContainText("repository Git configuration is unsafe", {
        timeout: DELIVERY_PROJECTION_CONVERGENCE_TIMEOUT_MS,
      });
      await expect(driver.panel()).not.toContainText("delivery-probe");
      await driver.assertDeliveryPanelRedactsAbsolutePaths();
      expect(await pathExists(sentinel)).toBe(false);
      await driver.git.assertTargetUnchanged(targetBefore);

      await driver.git.unsetLocalConfig("filter.delivery-probe.process");
      await driver.quit();
    },
  );
});

async function pathExists(candidate: string): Promise<boolean> {
  try {
    await access(candidate);
    return true;
  } catch {
    return false;
  }
}

function quotedGitHelperPath(candidate: string): string {
  const portable = candidate.replaceAll("\\", "/");
  if (portable.includes("\u0000") || /["$`]/u.test(portable)) {
    throw new Error("malicious-config fixture path cannot be quoted safely");
  }
  return `"${portable}"`;
}
