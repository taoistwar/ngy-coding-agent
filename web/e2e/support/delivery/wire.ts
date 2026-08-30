import type { Page } from "@playwright/test";

const POLL_TIMEOUT_MS = 180_000;
const POLL_INTERVAL_MS = 250;
const BROWSER_REQUEST_TIMEOUT_MARKER = "DELIVERY_E2E_BROWSER_REQUEST_TIMEOUT";
const MERGE_STATES = [
  "preflight_pending",
  "preflight_ready",
  "accepted",
  "merge_pending",
  "merged",
  "abort_pending",
  "conflict",
  "rejected",
  "stale",
  "superseded",
  "failed",
  "reconciliation_required",
] as const;

export type DeliveryMergeState = (typeof MERGE_STATES)[number];

export interface DeliveryMergeOperation {
  readonly kind?: "merge";
  readonly operation_id: string;
  readonly version: number;
  readonly state: DeliveryMergeState;
  readonly preflight_source_commit: string | null;
  readonly source_commit: string | null;
  readonly target_branch: string;
  readonly target_head: string;
}

export interface DeliveryCleanupOperation {
  readonly kind?: "cleanup";
  readonly operation_id: string;
  readonly cleanup_kind: "remove_worktree" | "delete_branch";
  readonly version: number;
  readonly state: string;
}

export type DeliveryOperation = DeliveryMergeOperation | DeliveryCleanupOperation;

export interface DeliveryTask {
  readonly task_id: string;
  readonly eligibility: "eligible" | "ineligible" | "unavailable";
  readonly reasons: readonly string[];
  readonly evidence: {
    readonly review_generation: number;
    readonly workspace_fingerprint: string;
  } | null;
  readonly target:
    | {
        readonly available: true;
        readonly branch: string;
        readonly head: string;
      }
    | {
        readonly available: false;
        readonly reason: string;
      };
  readonly source: {
    readonly state: string;
    readonly version: number;
    readonly source_ref: string;
    readonly source_oid: string | null;
  } | null;
  readonly latest_merge: DeliveryMergeOperation | null;
  readonly latest_cleanup: DeliveryCleanupOperation | null;
  readonly disposition: {
    readonly merged_operation_id: string;
    readonly source_ref: string;
    readonly source_oid: string;
    readonly worktree: { readonly state: string; readonly version: number };
    readonly branch: { readonly state: string; readonly version: number };
  } | null;
  readonly allowed_actions: readonly string[];
}

export interface TaskDeliverySummary {
  readonly id: string;
  readonly prompt: string;
  readonly status: string;
  readonly deliveryReadiness: string;
  readonly failureCode: string | null;
}

export async function fetchDelivery(
  page: Page,
  taskId: string,
  timeoutMs?: number,
): Promise<DeliveryTask> {
  return parseDelivery(
    await browserJson(
      page,
      `/api/tasks/${encodeURIComponent(taskId)}/delivery`,
      timeoutMs,
    ),
  );
}

export async function fetchDeliveryOperation(
  page: Page,
  operationId: string,
): Promise<DeliveryOperation> {
  return parseOperation(
    await browserJson(
      page,
      `/api/delivery-operations/${encodeURIComponent(operationId)}`,
    ),
    true,
  );
}

export async function waitForMergeState(
  page: Page,
  taskId: string,
  expected: DeliveryMergeState,
  timeoutMs = POLL_TIMEOUT_MS,
): Promise<DeliveryTask> {
  return waitForDelivery(
    page,
    taskId,
    (delivery) => delivery.latest_merge?.state === expected,
    `merge state ${expected}`,
    timeoutMs,
  );
}

export async function waitForDelivery(
  page: Page,
  taskId: string,
  predicate: (delivery: DeliveryTask) => boolean,
  label: string,
  timeoutMs = POLL_TIMEOUT_MS,
): Promise<DeliveryTask> {
  const startedAt = Date.now();
  const deadline = startedAt + timeoutMs;
  let last: DeliveryTask | null = null;
  while (true) {
    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) break;
    try {
      last = await fetchDelivery(page, taskId, remainingMs);
    } catch (error) {
      if (isBrowserRequestTimeout(error)) break;
      throw error;
    }
    if (predicate(last)) return last;
    await delayUntilNextPoll(deadline);
  }
  const elapsedMs = Date.now() - startedAt;
  throw new Error(
    `timed out after ${String(elapsedMs)} ms (budget ${String(timeoutMs)} ms) waiting for ${label}; last=${last === null ? "none" : JSON.stringify(last)}`,
  );
}

export async function waitForTaskDeliveryReadiness(
  page: Page,
  taskId: string,
  expected: "review_approved" | "review_rejected" | "unreviewed",
  timeoutMs = 180_000,
): Promise<TaskDeliverySummary> {
  const startedAt = Date.now();
  const deadline = startedAt + timeoutMs;
  let last: TaskDeliverySummary | null = null;
  while (true) {
    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) break;
    try {
      last = parseTaskSummary(
        await browserJson(
          page,
          `/api/tasks/${encodeURIComponent(taskId)}`,
          remainingMs,
        ),
      );
    } catch (error) {
      if (isBrowserRequestTimeout(error)) break;
      throw error;
    }
    if (last.status === "completed" && last.deliveryReadiness === expected) return last;
    if (["failed", "cancelled", "interrupted"].includes(last.status)) {
      throw new Error(
        `task became ${last.status} before ${expected}; failure=${last.failureCode ?? "none"}`,
      );
    }
    await delayUntilNextPoll(deadline);
  }
  const elapsedMs = Date.now() - startedAt;
  throw new Error(
    `timed out after ${String(elapsedMs)} ms (budget ${String(timeoutMs)} ms) waiting for completed + ${expected}; last=${last === null ? "none" : JSON.stringify(last)}`,
  );
}

export async function browserJson(
  page: Page,
  requestPath: string,
  timeoutMs?: number,
): Promise<unknown> {
  const requestTimeoutMs = timeoutMs === undefined ? null : Math.max(1, Math.floor(timeoutMs));
  return page.evaluate(async ({ path_, timeoutMs_, timeoutMarker_ }) => {
    const controller = timeoutMs_ === null ? null : new AbortController();
    const timer = controller === null || timeoutMs_ === null
      ? null
      : window.setTimeout(() => controller.abort(), timeoutMs_);
    try {
      const response = await fetch(path_, {
        credentials: "same-origin",
        headers: { accept: "application/json" },
        signal: controller?.signal ?? null,
      });
      if (!response.ok) {
        throw new Error(`GET ${path_} failed with HTTP ${String(response.status)}`);
      }
      return response.json() as Promise<unknown>;
    } catch (error) {
      if (controller?.signal.aborted === true) {
        throw new Error(`${timeoutMarker_}:${String(timeoutMs_)}`);
      }
      throw error;
    } finally {
      if (timer !== null) window.clearTimeout(timer);
    }
  }, {
    path_: requestPath,
    timeoutMs_: requestTimeoutMs,
    timeoutMarker_: BROWSER_REQUEST_TIMEOUT_MARKER,
  });
}

function parseTaskSummary(value: unknown): TaskDeliverySummary {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("task detail response is not an object");
  }
  const detail = value as Record<string, unknown>;
  const taskValue = detail.task;
  if (typeof taskValue !== "object" || taskValue === null || Array.isArray(taskValue)) {
    throw new Error("task detail response is missing task");
  }
  const task = taskValue as Record<string, unknown>;
  const failureCode = task.failure === null
    ? null
    : stringValue(record(task.failure, "task.failure").code, "task.failure.code");
  if (
    typeof task.id !== "string" ||
    typeof task.prompt !== "string" ||
    typeof task.status !== "string" ||
    typeof task.delivery_readiness !== "string"
  ) {
    throw new Error("task detail response has an invalid task summary");
  }
  return Object.freeze({
    id: task.id,
    prompt: task.prompt,
    status: task.status,
    deliveryReadiness: task.delivery_readiness,
    failureCode,
  });
}

function parseDelivery(value: unknown): DeliveryTask {
  const task = record(value, "delivery projection");
  requireExactKeys(task, [
    "allowed_actions",
    "disposition",
    "eligibility",
    "evidence",
    "latest_cleanup",
    "latest_merge",
    "reasons",
    "source",
    "target",
    "task_id",
  ]);
  stringValue(task.task_id, "task_id");
  enumValue(task.eligibility, "eligibility", ["eligible", "ineligible", "unavailable"]);
  stringArray(task.reasons, "reasons");
  stringArray(task.allowed_actions, "allowed_actions");
  if (task.evidence !== null) {
    const evidence = record(task.evidence, "evidence");
    numberValue(evidence.review_generation, "evidence.review_generation");
    stringValue(evidence.workspace_fingerprint, "evidence.workspace_fingerprint");
  }
  parseTarget(task.target);
  if (task.source !== null) parseSource(task.source);
  if (task.latest_merge !== null) parseMergeOperation(task.latest_merge, false);
  if (task.latest_cleanup !== null) parseCleanupOperation(task.latest_cleanup, false);
  if (task.disposition !== null) parseDisposition(task.disposition);
  return value as DeliveryTask;
}

function parseOperation(value: unknown, envelope: boolean): DeliveryOperation {
  const operation = record(value, "delivery operation");
  const kind = envelope ? enumValue(operation.kind, "operation.kind", ["merge", "cleanup"]) : null;
  return kind === "cleanup"
    ? parseCleanupOperation(operation, true)
    : parseMergeOperation(operation, envelope);
}

function parseMergeOperation(value: unknown, envelope: boolean): DeliveryMergeOperation {
  const operation = record(value, "merge operation");
  if (envelope) enumValue(operation.kind, "operation.kind", ["merge"]);
  stringValue(operation.operation_id, "operation.operation_id");
  numberValue(operation.version, "operation.version");
  enumValue(operation.state, "operation.state", MERGE_STATES);
  nullableString(operation.preflight_source_commit, "operation.preflight_source_commit");
  nullableString(operation.source_commit, "operation.source_commit");
  stringValue(operation.target_branch, "operation.target_branch");
  stringValue(operation.target_head, "operation.target_head");
  return value as DeliveryMergeOperation;
}

function parseCleanupOperation(value: unknown, envelope: boolean): DeliveryCleanupOperation {
  const operation = record(value, "cleanup operation");
  if (envelope) enumValue(operation.kind, "operation.kind", ["cleanup"]);
  stringValue(operation.operation_id, "operation.operation_id");
  enumValue(operation.cleanup_kind, "operation.cleanup_kind", [
    "remove_worktree",
    "delete_branch",
  ]);
  numberValue(operation.version, "operation.version");
  stringValue(operation.state, "operation.state");
  return value as DeliveryCleanupOperation;
}

function parseTarget(value: unknown): void {
  const target = record(value, "target");
  if (target.available === true) {
    stringValue(target.branch, "target.branch");
    stringValue(target.head, "target.head");
    return;
  }
  if (target.available !== false) throw new Error("target.available is not boolean");
  stringValue(target.reason, "target.reason");
}

function parseSource(value: unknown): void {
  const source = record(value, "source");
  stringValue(source.state, "source.state");
  numberValue(source.version, "source.version");
  stringValue(source.source_ref, "source.source_ref");
  nullableString(source.source_oid, "source.source_oid");
}

function parseDisposition(value: unknown): void {
  const disposition = record(value, "disposition");
  stringValue(disposition.merged_operation_id, "disposition.merged_operation_id");
  stringValue(disposition.source_ref, "disposition.source_ref");
  stringValue(disposition.source_oid, "disposition.source_oid");
  for (const key of ["worktree", "branch"] as const) {
    const state = record(disposition[key], `disposition.${key}`);
    stringValue(state.state, `disposition.${key}.state`);
    numberValue(state.version, `disposition.${key}.version`);
  }
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} is not an object`);
  }
  return value as Record<string, unknown>;
}

function stringValue(value: unknown, label: string): string {
  if (typeof value !== "string" || value === "") throw new Error(`${label} is not a string`);
  return value;
}

function nullableString(value: unknown, label: string): string | null {
  return value === null ? null : stringValue(value, label);
}

function numberValue(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw new Error(`${label} is not a non-negative safe integer`);
  }
  return Number(value);
}

function stringArray(value: unknown, label: string): readonly string[] {
  if (!Array.isArray(value)) throw new Error(`${label} is not an array`);
  return value.map((item, index) => stringValue(item, `${label}[${String(index)}]`));
}

function enumValue<const T>(value: unknown, label: string, allowed: readonly T[]): T {
  if (!allowed.some((item) => Object.is(item, value))) {
    throw new Error(`${label} is not one of ${allowed.map(String).join(", ")}`);
  }
  return value as T;
}

function requireExactKeys(value: Record<string, unknown>, expected: readonly string[]): void {
  const actual = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  if (
    actual.length !== sortedExpected.length ||
    actual.some((key, index) => key !== sortedExpected[index])
  ) {
    throw new Error("delivery projection contains missing or unknown fields");
  }
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function delayUntilNextPoll(deadline: number): Promise<void> {
  const remainingMs = deadline - Date.now();
  if (remainingMs > 0) await delay(Math.min(POLL_INTERVAL_MS, remainingMs));
}

function isBrowserRequestTimeout(error: unknown): boolean {
  return (
    error instanceof Error && error.message.includes(BROWSER_REQUEST_TIMEOUT_MARKER)
  );
}
