import type { Page } from "@playwright/test";

import type {
  ProcessScenario,
  ScenarioRoots,
} from "./localApp";

export type StorageState = "normal" | "pressure" | "critical" | "unavailable";

export type StorageTaskStatus =
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "interrupted";

export interface StorageTaskFailure {
  code: string;
  message: string;
  retryable: boolean;
}

export interface StorageTask {
  id: string;
  prompt: string;
  status: StorageTaskStatus;
  failure: StorageTaskFailure | null;
}

export interface StorageSchedulerSnapshot {
  storageState: StorageState;
  queuedTasks: Array<{
    taskId: string;
    reason: string;
  }>;
}

export interface StorageReleasePaths {
  pressure: string;
  unavailable: string;
  recovery: string;
  critical: string;
}

const GIBIBYTE = 1024 ** 3;
const MEBIBYTE = 1024 ** 2;

export function storagePressureScenario(roots: ScenarioRoots): {
  scenario: ProcessScenario;
  releasePaths: StorageReleasePaths;
} {
  const releasePaths = {
    pressure: roots.releaseSignalPath("storage-pressure"),
    unavailable: roots.releaseSignalPath("storage-unavailable"),
    recovery: roots.releaseSignalPath("storage-recovery"),
    critical: roots.releaseSignalPath("storage-critical"),
  };
  return {
    scenario: {
      runtime_config: null,
      fake_scenarios: ["blocking", "blocking"],
      storage_samples: [
        { kind: "available", available_bytes: 16 * GIBIBYTE },
        { kind: "available", available_bytes: 600 * MEBIBYTE },
        { kind: "unavailable" },
        { kind: "available", available_bytes: 16 * GIBIBYTE },
        { kind: "available", available_bytes: 32 * MEBIBYTE },
      ],
      store_writer_faults: [],
      actor_pauses: [],
      virtual_release_signals: [
        {
          name: "storage-pressure",
          path: releasePaths.pressure,
          target: "storage_next",
        },
        {
          name: "storage-unavailable",
          path: releasePaths.unavailable,
          target: "storage_next",
        },
        {
          name: "storage-recovery",
          path: releasePaths.recovery,
          target: "storage_next",
        },
        {
          name: "storage-critical",
          path: releasePaths.critical,
          target: "storage_next",
        },
      ],
      legacy_v2_seed: { kind: "none" },
      marker_write_failure: false,
    },
    releasePaths,
  };
}

export async function listStorageTasks(page: Page): Promise<StorageTask[]> {
  const value = await page.evaluate(async () => {
    const response = await fetch("/api/tasks", {
      credentials: "same-origin",
      headers: { accept: "application/json" },
    });
    if (!response.ok) {
      throw new Error(`task list failed with HTTP ${String(response.status)}`);
    }
    return response.json() as Promise<unknown>;
  });
  if (!Array.isArray(value)) {
    throw new Error("task list response is not an array");
  }
  return value.map(storageTask);
}

export async function readStorageScheduler(
  page: Page,
): Promise<StorageSchedulerSnapshot> {
  const value = await page.evaluate(async () => {
    const response = await fetch("/api/bootstrap", {
      credentials: "same-origin",
      headers: { accept: "application/json" },
    });
    if (!response.ok) {
      throw new Error(`bootstrap failed with HTTP ${String(response.status)}`);
    }
    return response.json() as Promise<unknown>;
  });
  if (!isRecord(value) || !isRecord(value.scheduler)) {
    throw new Error("bootstrap scheduler response is invalid");
  }
  const scheduler = value.scheduler;
  if (!isRecord(scheduler.storage) || !isStorageState(scheduler.storage.state)) {
    throw new Error("bootstrap scheduler storage response is invalid");
  }
  if (!Array.isArray(scheduler.queued_tasks)) {
    throw new Error("bootstrap scheduler queue response is invalid");
  }
  return {
    storageState: scheduler.storage.state,
    queuedTasks: scheduler.queued_tasks.map(queuedTask),
  };
}

export function storageTask(value: unknown): StorageTask {
  if (
    !isRecord(value) ||
    typeof value.id !== "string" ||
    typeof value.prompt !== "string" ||
    !isStorageTaskStatus(value.status)
  ) {
    throw new Error("task response is invalid");
  }
  return {
    id: value.id,
    prompt: value.prompt,
    status: value.status,
    failure: storageTaskFailure(value.failure),
  };
}

function queuedTask(value: unknown): {
  taskId: string;
  reason: string;
} {
  if (
    !isRecord(value) ||
    typeof value.task_id !== "string" ||
    typeof value.reason !== "string"
  ) {
    throw new Error("bootstrap queued-task response is invalid");
  }
  return {
    taskId: value.task_id,
    reason: value.reason,
  };
}

function storageTaskFailure(value: unknown): StorageTaskFailure | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (
    !isRecord(value) ||
    typeof value.code !== "string" ||
    typeof value.message !== "string" ||
    typeof value.retryable !== "boolean"
  ) {
    throw new Error("task failure response is invalid");
  }
  return {
    code: value.code,
    message: value.message,
    retryable: value.retryable,
  };
}

function isStorageState(value: unknown): value is StorageState {
  return (
    value === "normal" ||
    value === "pressure" ||
    value === "critical" ||
    value === "unavailable"
  );
}

function isStorageTaskStatus(value: unknown): value is StorageTaskStatus {
  return (
    value === "queued" ||
    value === "running" ||
    value === "completed" ||
    value === "failed" ||
    value === "cancelled" ||
    value === "interrupted"
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
