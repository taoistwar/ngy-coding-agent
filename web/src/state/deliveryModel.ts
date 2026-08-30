import type {
  DeliveryOperation,
  DeliveryTask,
} from "../api/types";

export const DELIVERY_INITIAL_POLL_DELAY_MS = 500;
export const DELIVERY_MAX_POLL_DELAY_MS = 2_000;

const SCHEDULER_OWNED_ELIGIBILITY_REASONS = new Set<
  DeliveryTask["reasons"][number]
>(["task_active", "repository_busy"]);

export type DeliveryPhase =
  | "idle"
  | "loading"
  | "ready"
  | "polling"
  | "refreshing"
  | "error";

export interface DeliveryErrorState {
  code: string;
  message: string;
  retryable: boolean;
  requestId: string | null;
}

export type DeliveryModalKind =
  | "preflight"
  | "merge"
  | "remove_worktree"
  | "delete_branch";

export interface DeliveryModalAuthority {
  reviewGeneration: number;
  workspaceFingerprint: string;
  targetBranch: string;
  targetHead: string;
}

export interface DeliveryModalState {
  kind: DeliveryModalKind;
  taskId: string;
  operationId: string | null;
  operationVersion: number | null;
  authority: DeliveryModalAuthority | null;
}

export interface DeliveryState {
  taskId: string | null;
  generation: number;
  phase: DeliveryPhase;
  projection: DeliveryTask | null;
  operation: DeliveryOperation | null;
  trackedOperationId: string | null;
  pollDelayMs: number;
  error: DeliveryErrorState | null;
  modal: DeliveryModalState | null;
}

export const initialDeliveryState: DeliveryState = {
  taskId: null,
  generation: 0,
  phase: "idle",
  projection: null,
  operation: null,
  trackedOperationId: null,
  pollDelayMs: DELIVERY_INITIAL_POLL_DELAY_MS,
  error: null,
  modal: null,
};

export function shouldPollDeliveryOperation(operation: DeliveryOperation): boolean {
  if (operation.kind === "merge") {
    return (
      operation.state === "preflight_pending" ||
      operation.state === "accepted" ||
      operation.state === "merge_pending" ||
      operation.state === "abort_pending"
    );
  }
  return (
    operation.state === "unlock_pending" ||
    operation.state === "unlocked_pending_remove" ||
    operation.state === "remove_pending" ||
    operation.state === "delete_pending"
  );
}

export function shouldRefreshDeliveryAfterSchedulerChange(task: DeliveryTask): boolean {
  return (
    task.eligibility !== "eligible" &&
    task.reasons.some((reason) => SCHEDULER_OWNED_ELIGIBILITY_REASONS.has(reason))
  );
}

export function mergeEnvelopeFromTask(task: DeliveryTask): DeliveryOperation | null {
  return task.latest_merge === null
    ? null
    : { kind: "merge", ...task.latest_merge };
}

export function cleanupEnvelopeFromTask(task: DeliveryTask): DeliveryOperation | null {
  return task.latest_cleanup === null
    ? null
    : { kind: "cleanup", ...task.latest_cleanup };
}

export function latestOperationFromTask(task: DeliveryTask): DeliveryOperation | null {
  return cleanupEnvelopeFromTask(task) ?? mergeEnvelopeFromTask(task);
}
