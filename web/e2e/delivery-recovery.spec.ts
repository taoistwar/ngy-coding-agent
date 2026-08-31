import { lstat } from "node:fs/promises";
import path from "node:path";

import { expect, test } from "./fixtures";

import {
  DeliveryBrowserDriver,
  fetchDelivery,
  PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
  productionDeliveryScenario,
  productionDeliveryTestTimeout,
  waitForDelivery,
} from "./support/delivery";
import {
  publishReleaseSignal,
  waitForReachedSignal,
  withLocalApp,
  type LocalApp,
  type ProcessDeliveryProviderScenario,
  type ReachedSignal,
  type StoreWriterOperation,
} from "./support/localApp";

// The test-only pre-actor pause splits startup into two serial waits: reaching
// the pause consumes harness budget, and delivery recovery runs after release.
const RECOVERY_STARTUP_TIMEOUT_MS = 2 * PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS;
// The merge click reaches the durable abort receipt through accept
// authentication, source construction, and the conflict-producing merge.
const CONFLICT_ABORT_REACHED_TIMEOUT_MS = 3 * PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS;
const CONFLICT_ABORT_TEST_TIMEOUT_MS =
  productionDeliveryTestTimeout("approval", "preflight", "merge") +
  CONFLICT_ABORT_REACHED_TIMEOUT_MS -
  PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS;

test("hard-kill source recovery completes the merge before Ready, then recovers both cleanup receipts", async (
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
  const sourcePause = "delivery-source-object-pending";
  await withLocalApp(
    testInfo,
    (roots) =>
      productionDeliveryScenario(roots, {
        providerScenario: "approve",
        pauseAfterCommit: { name: sourcePause, operation: "create_delivery_source" },
      }),
    async (app) => {
      const driver = await DeliveryBrowserDriver.open(page, app);
      await driver.registerFixtureRepository();
      const task = await driver.createApprovedTask(
        "Recover each durable local delivery receipt exactly once",
      );
      const targetBefore = await driver.git.snapshotTarget();
      const ready = await driver.runPreflight(task.id, targetBefore);
      const preflightSourceCommit = ready.latest_merge?.preflight_source_commit;
      if (preflightSourceCommit === null || preflightSourceCommit === undefined) {
        throw new Error("ready preflight omitted source commit before source recovery");
      }

      await driver.startMergeWithoutWaiting();
      await crashAtReachedStorePause(app, sourcePause, async () => {
        const pending = await fetchDelivery(
          page,
          task.id,
          PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
        );
        expect(pending.latest_merge?.state).toBe("accepted");
        expect(pending.source?.state).toBe("object_pending");
      });
      await restartAtRecoveryBoundary(app, "source-recovery-before-ready");
      await driver.reopen();
      await driver.selectTask(task.prompt);
      const merged = await driver.waitForMergeState(task.id, "merged");
      const sourceRef = merged.disposition?.source_ref;
      const sourceOid = merged.disposition?.source_oid;
      if (sourceRef === undefined || sourceOid === undefined) {
        throw new Error("recovered merge omitted retained source identity");
      }
      const exactMerge = await driver.git.assertExactNoFfMerge(targetBefore, sourceOid);
      expect(exactMerge.secondParent).toBe(merged.latest_merge?.source_commit);
      expect(exactMerge.secondParent).toBe(merged.source?.source_oid);
      expect(exactMerge.secondParent).not.toBe(preflightSourceCommit);
      await driver.git.removeFixtureCargoTarget(sourceRef, sourceOid);

      const removePause = "delivery-worktree-unlock-pending";
      await restartWithStorePause(
        app,
        driver,
        task.prompt,
        removePause,
        "accept_worktree_cleanup",
      );
      await driver.startRemoveWorktreeWithoutWaiting();
      await crashAtReachedStorePause(app, removePause, async () => {
        const pending = await fetchDelivery(
          page,
          task.id,
          PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
        );
        expect(pending.latest_cleanup?.state).toBe("unlock_pending");
      });
      await restartAtRecoveryBoundary(app, "worktree-recovery-before-ready");
      await driver.reopen();
      await driver.selectTask(task.prompt);
      const removed = await waitForDelivery(
        page,
        task.id,
        (delivery) => delivery.disposition?.worktree.state === "removed",
        "recovered worktree removal",
        PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
      );
      expect(removed.disposition?.branch.state).toBe("retained");
      expect(await driver.git.refOid(sourceRef)).toBe(sourceOid);

      const deletePause = "delivery-branch-delete-pending";
      await restartWithStorePause(
        app,
        driver,
        task.prompt,
        deletePause,
        "accept_branch_cleanup",
      );
      await driver.startDeleteBranchWithoutWaiting();
      await crashAtReachedStorePause(app, deletePause, async () => {
        const pending = await fetchDelivery(
          page,
          task.id,
          PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
        );
        expect(pending.latest_cleanup?.state).toBe("delete_pending");
      });
      await restartAtRecoveryBoundary(app, "branch-recovery-before-ready");
      await driver.reopen();
      await driver.selectTask(task.prompt);
      const deleted = await waitForDelivery(
        page,
        task.id,
        (delivery) => delivery.disposition?.branch.state === "deleted",
        "recovered branch deletion",
        PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
      );
      expect(deleted.disposition?.worktree.state).toBe("removed");
      expect(await driver.git.refOid(sourceRef)).toBeNull();
      await driver.quit();
    },
  );
});

test("hard-kill merge-pending recovery reaches one exact no-ff merge before Ready", async (
  { page },
  testInfo,
) => {
  test.setTimeout(productionDeliveryTestTimeout("approval", "preflight", "merge"));
  const mergePause = "delivery-merge-pending";
  await withLocalApp(
    testInfo,
    (roots) =>
      productionDeliveryScenario(roots, {
        providerScenario: "approve",
        pauseAfterCommit: { name: mergePause, operation: "enter_merge_pending" },
      }),
    async (app) => {
      const driver = await DeliveryBrowserDriver.open(page, app);
      await driver.registerFixtureRepository();
      const task = await driver.createApprovedTask(
        "Recover a durable merge-pending receipt into one exact no-ff merge",
      );
      const targetBefore = await driver.git.snapshotTarget();
      const ready = await driver.runPreflight(task.id, targetBefore);
      const preflightSourceCommit = ready.latest_merge?.preflight_source_commit;
      if (preflightSourceCommit === null || preflightSourceCommit === undefined) {
        throw new Error("ready preflight omitted source commit before merge recovery");
      }

      await driver.startMergeWithoutWaiting();
      await crashAtReachedStorePause(app, mergePause, async () => {
        const pending = await fetchDelivery(
          page,
          task.id,
          PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
        );
        expect(pending.latest_merge?.state).toBe("merge_pending");
      });
      await restartAtRecoveryBoundary(app, "merge-recovery-before-ready");
      await driver.reopen();
      await driver.selectTask(task.prompt);
      const merged = await driver.waitForMergeState(task.id, "merged");
      const durableSourceCommit = merged.disposition?.source_oid;
      if (durableSourceCommit === undefined) {
        throw new Error("recovered merge omitted its durable source commit");
      }
      const exactMerge = await driver.git.assertExactNoFfMerge(
        targetBefore,
        durableSourceCommit,
      );
      expect(exactMerge.secondParent).toBe(merged.latest_merge?.source_commit);
      expect(exactMerge.secondParent).toBe(merged.source?.source_oid);
      expect(exactMerge.secondParent).not.toBe(preflightSourceCommit);
      await driver.quit();
    },
  );
});

test("hard-kill recovery finishes an exact conflict abort without changing target bytes", async (
  { page },
  testInfo,
) => {
  test.setTimeout(CONFLICT_ABORT_TEST_TIMEOUT_MS);
  const abortPause = "delivery-abort-pending";
  await withLocalApp(
    testInfo,
    (roots) =>
      productionDeliveryScenario(roots, {
        providerScenario: "runtime_conflict",
        pauseAfterCommit: { name: abortPause, operation: "begin_merge_abort" },
      }),
    async (app) => {
      const driver = await DeliveryBrowserDriver.open(page, app);
      await driver.registerFixtureRepository();
      const task = await driver.createApprovedTask(
        "Recover an interrupted conflict abort without touching the target checkout",
      );
      await driver.git.writeFixtureFile(
        "src/runtime_conflict.rs",
        'pub const SOURCE_SIDE: &str = "base";\n\npub const TARGET_SIDE: &str = "target";\n',
      );
      await driver.git.commitAll("create target conflict for abort recovery");
      const targetBefore = await driver.git.snapshotTarget();
      await page.reload();
      await expect(page.locator(".connection-banner")).toContainText("Connected");
      await driver.selectTask(task.prompt);

      const ready = await driver.runPreflight(task.id, targetBefore);
      expect(ready.latest_merge?.state).toBe("preflight_ready");
      await driver.startMergeWithoutWaiting();
      await crashAtReachedStorePause(
        app,
        abortPause,
        async () => {
          const pending = await fetchDelivery(
            page,
            task.id,
            PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
          );
          expect(pending.latest_merge?.state).toBe("abort_pending");
        },
        CONFLICT_ABORT_REACHED_TIMEOUT_MS,
      );
      await restartAtRecoveryBoundary(app, "abort-recovery-before-ready", "runtime_conflict");
      await driver.reopen();
      await driver.selectTask(task.prompt);
      await driver.waitForMergeState(task.id, "conflict");
      await driver.git.assertTargetUnchanged(targetBefore);
      await expect(driver.panel()).toContainText("Preflight found conflicts");
      await driver.assertDeliveryPanelRedactsAbsolutePaths();
      await driver.quit();
    },
  );
});

async function restartWithStorePause(
  app: LocalApp,
  driver: DeliveryBrowserDriver,
  prompt: string,
  signalName: string,
  operation: StoreWriterOperation,
): Promise<void> {
  await app.hardKillPrimaryPreservingRoot();
  await app.restart(
    (roots) =>
      productionDeliveryScenario(roots, {
        providerScenario: "approve",
        pauseAfterCommit: { name: signalName, operation },
      }),
    { startupTimeoutMs: PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS },
  );
  await driver.reopen();
  await driver.selectTask(prompt);
}

async function crashAtReachedStorePause(
  app: LocalApp,
  signalName: string,
  assertPersistedPending: () => Promise<void>,
  reachedTimeoutMs = PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
): Promise<void> {
  const releasePath = path.join(app.runtimeDir, "signals", `${signalName}.release`);
  await waitForReachedSignal(
    app.runtimeDir,
    releasePath,
    reachedTimeoutMs,
  );
  const failures: unknown[] = [];
  try {
    await assertPersistedPending();
  } catch (error) {
    failures.push(error);
  }
  try {
    await app.hardKillPrimaryPreservingRoot();
  } catch (error) {
    failures.push(error);
  }
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1) {
    throw new AggregateError(
      failures,
      "persisted pending assertion and hard-kill both failed",
    );
  }
}

async function restartAtRecoveryBoundary(
  app: LocalApp,
  signalName: string,
  providerScenario: ProcessDeliveryProviderScenario = "approve",
): Promise<void> {
  const releasePath = path.join(app.runtimeDir, "signals", `${signalName}.release`);
  const restart = app.restart(
    (roots) =>
      productionDeliveryScenario(roots, {
        providerScenario,
        pauseBeforeDescriptor: { name: signalName },
      }),
    { startupTimeoutMs: RECOVERY_STARTUP_TIMEOUT_MS },
  );
  void restart.catch(() => undefined);
  let reached: ReachedSignal | null = null;
  let markerFailure: unknown = null;
  try {
    reached = await waitForReachedSignal(
      app.runtimeDir,
      releasePath,
      PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS,
    );
  } catch (error) {
    markerFailure = error;
  }
  let boundaryFailure: unknown = null;
  if (reached !== null) {
    try {
      await expectPathAbsent(app.descriptorPath);
    } catch (error) {
      boundaryFailure = error;
    }
  }
  let releaseFailure: unknown = null;
  if (reached !== null) {
    try {
      await publishReleaseSignal(reached);
    } catch (error) {
      releaseFailure = error;
    }
  }
  let restartFailure: unknown = null;
  try {
    await restart;
  } catch (error) {
    restartFailure = error;
  }
  const failures = [markerFailure, boundaryFailure, releaseFailure, restartFailure].filter(
    (error) => error !== null,
  );
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1) {
    throw new AggregateError(failures, "delivery recovery boundary failed");
  }
}

async function expectPathAbsent(target: string): Promise<void> {
  try {
    await lstat(target);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw error;
  }
  throw new Error("runtime descriptor was published before delivery recovery completed");
}
