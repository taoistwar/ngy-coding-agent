const MILLISECONDS_PER_MINUTE = 60_000;
const TEST_HARNESS_STAGE_ALLOWANCE = 1;

// The production actor waits 11 minutes around each runtime stage: ten minutes
// for the child plus one minute for the runtime to prove process-tree cleanup.
export const PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS = 11 * MILLISECONDS_PER_MINUTE;

export type ProductionDeliveryScenarioStage =
  | "approval"
  | "preflight"
  | "merge"
  | "worktree_cleanup"
  | "branch_cleanup";

/**
 * Bounds a real-process scenario by its named production stages and reserves
 * one additional stage for app startup, browser assertions, and clean shutdown.
 */
export function productionDeliveryTestTimeout(
  ...stages: readonly ProductionDeliveryScenarioStage[]
): number {
  if (stages.length === 0) {
    throw new Error("production delivery test timeout requires at least one stage");
  }
  return (stages.length + TEST_HARNESS_STAGE_ALLOWANCE) * PRODUCTION_DELIVERY_STAGE_TIMEOUT_MS;
}
