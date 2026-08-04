import type {
  SchedulerControlStorage,
  SchedulerLimits,
  SchedulerStateChunkControl,
  SchedulerStateControl,
  SchedulerStateItem,
  SchedulerStorageScope,
} from "../types";
import { isValidRfc3339UtcTimestamp } from "../schedulerValidation";
import { fail } from "./error";

const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;
const MAX_U32 = 0xffff_ffff;
const MAX_ITEMS_PER_CHUNK = 128;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const DIGEST = /^[0-9a-f]{64}$/;

const ADMISSION_STATES = ["running", "paused"] as const;
const STORAGE_STATES = [
  "normal",
  "pressure",
  "critical",
  "unavailable",
] as const;
const QUEUE_REASONS = [
  "service_paused",
  "storage_pressure",
  "global_capacity",
  "repository_capacity",
  "repository_control_busy",
] as const;
const STOP_INTENTS = ["user_cancelled", "disk_pressure_critical"] as const;

export type SchedulerWireControl =
  | SchedulerStateControl
  | SchedulerStateChunkControl;

export function validateSchedulerStateControl(
  value: unknown,
): SchedulerStateControl {
  const manifest = exactObject(value, "$.scheduler_control", [
    "schema_version",
    "kind",
    "server_instance_id",
    "server_started_at",
    "generation",
    "as_of_event_id",
    "service_state_generation",
    "admission_state",
    "limits",
    "active_task_count",
    "queued_task_count",
    "stopping_task_count",
    "repository_storage_count",
    "storage",
    "item_count",
    "chunk_count",
    "snapshot_digest",
  ]);
  exactInteger(manifest.schema_version, "$.scheduler_control.schema_version", 1);
  exactString(manifest.kind, "$.scheduler_control.kind", "scheduler.state");
  canonicalUuidV4(
    manifest.server_instance_id,
    "$.scheduler_control.server_instance_id",
  );
  utcTimestamp(
    manifest.server_started_at,
    "$.scheduler_control.server_started_at",
  );
  nonNegativeSafeInteger(
    manifest.generation,
    "$.scheduler_control.generation",
  );
  nonNegativeSafeInteger(
    manifest.as_of_event_id,
    "$.scheduler_control.as_of_event_id",
  );
  nonNegativeSafeInteger(
    manifest.service_state_generation,
    "$.scheduler_control.service_state_generation",
  );
  enumValue(
    manifest.admission_state,
    "$.scheduler_control.admission_state",
    ADMISSION_STATES,
  );
  const limits = readLimits(manifest.limits, "$.scheduler_control.limits");
  const active = integerBetween(
    manifest.active_task_count,
    "$.scheduler_control.active_task_count",
    0,
    4,
  );
  const queued = u32(
    manifest.queued_task_count,
    "$.scheduler_control.queued_task_count",
  );
  const stopping = integerBetween(
    manifest.stopping_task_count,
    "$.scheduler_control.stopping_task_count",
    0,
    4,
  );
  const repositories = u32(
    manifest.repository_storage_count,
    "$.scheduler_control.repository_storage_count",
  );
  readControlStorage(manifest.storage, "$.scheduler_control.storage");
  const itemCount = u32(
    manifest.item_count,
    "$.scheduler_control.item_count",
  );
  const chunkCount = u32(
    manifest.chunk_count,
    "$.scheduler_control.chunk_count",
  );
  lowercaseDigest(
    manifest.snapshot_digest,
    "$.scheduler_control.snapshot_digest",
  );

  if (active > limits.global) {
    fail("$.scheduler_control.active_task_count", "must not exceed limits.global");
  }
  if (stopping > active) {
    fail(
      "$.scheduler_control.stopping_task_count",
      "must not exceed active_task_count",
    );
  }
  const declaredItems = queued + stopping + repositories;
  if (!Number.isSafeInteger(declaredItems) || declaredItems > MAX_U32) {
    fail("$.scheduler_control.item_count", "declared item counts overflow u32");
  }
  if (itemCount !== declaredItems) {
    fail(
      "$.scheduler_control.item_count",
      "must equal queued, stopping, and repository counts",
    );
  }
  if (chunkCount !== Math.ceil(itemCount / MAX_ITEMS_PER_CHUNK)) {
    fail(
      "$.scheduler_control.chunk_count",
      "must equal ceil(item_count / 128)",
    );
  }
  return value as SchedulerStateControl;
}

export function validateSchedulerStateChunkControl(
  value: unknown,
): SchedulerStateChunkControl {
  const chunk = exactObject(value, "$.scheduler_chunk", [
    "schema_version",
    "kind",
    "server_instance_id",
    "generation",
    "snapshot_digest",
    "chunk_index",
    "chunk_count",
    "items",
  ]);
  exactInteger(chunk.schema_version, "$.scheduler_chunk.schema_version", 1);
  exactString(chunk.kind, "$.scheduler_chunk.kind", "scheduler.state.chunk");
  canonicalUuidV4(
    chunk.server_instance_id,
    "$.scheduler_chunk.server_instance_id",
  );
  nonNegativeSafeInteger(chunk.generation, "$.scheduler_chunk.generation");
  lowercaseDigest(
    chunk.snapshot_digest,
    "$.scheduler_chunk.snapshot_digest",
  );
  const chunkIndex = u32(chunk.chunk_index, "$.scheduler_chunk.chunk_index");
  const chunkCount = integerBetween(
    chunk.chunk_count,
    "$.scheduler_chunk.chunk_count",
    1,
    MAX_U32,
  );
  if (chunkIndex >= chunkCount) {
    fail("$.scheduler_chunk.chunk_index", "must be less than chunk_count");
  }
  const rawItems = readArray(chunk.items, "$.scheduler_chunk.items");
  if (rawItems.length === 0 || rawItems.length > MAX_ITEMS_PER_CHUNK) {
    fail("$.scheduler_chunk.items", "must contain between 1 and 128 items");
  }
  rawItems.forEach((item, index) =>
    readItem(item, `$.scheduler_chunk.items[${index}]`),
  );
  return value as SchedulerStateChunkControl;
}

function readLimits(value: unknown, path: string): SchedulerLimits {
  const limits = exactObject(value, path, [
    "global",
    "per_repository",
    "queued",
    "cargo_jobs_per_task",
  ]);
  const global = integerBetween(limits.global, `${path}.global`, 1, 4);
  const perRepository = integerBetween(
    limits.per_repository,
    `${path}.per_repository`,
    1,
    4,
  );
  if (perRepository > global) {
    fail(`${path}.per_repository`, "must not exceed global");
  }
  integerBetween(limits.queued, `${path}.queued`, 1, 256);
  integerBetween(
    limits.cargo_jobs_per_task,
    `${path}.cargo_jobs_per_task`,
    1,
    8,
  );
  return value as SchedulerLimits;
}

function readControlStorage(
  value: unknown,
  path: string,
): SchedulerControlStorage {
  const storage = exactObject(value, path, ["state", "data", "runtime"]);
  enumValue(storage.state, `${path}.state`, STORAGE_STATES);
  readStorageScope(storage.data, `${path}.data`);
  readStorageScope(storage.runtime, `${path}.runtime`);
  return value as SchedulerControlStorage;
}

function readStorageScope(value: unknown, path: string): SchedulerStorageScope {
  const scope = exactObject(value, path, ["state"]);
  enumValue(scope.state, `${path}.state`, STORAGE_STATES);
  return value as SchedulerStorageScope;
}

function readItem(value: unknown, path: string): SchedulerStateItem {
  const discriminator = isRecord(value) ? value.kind : undefined;
  if (discriminator === "queued_task") {
    const item = exactObject(value, path, ["kind", "task_id", "reason"]);
    canonicalUuid(item.task_id, `${path}.task_id`);
    enumValue(item.reason, `${path}.reason`, QUEUE_REASONS);
    return value as SchedulerStateItem;
  }
  if (discriminator === "stopping_task") {
    const item = exactObject(value, path, ["kind", "task_id", "intent"]);
    canonicalUuid(item.task_id, `${path}.task_id`);
    enumValue(item.intent, `${path}.intent`, STOP_INTENTS);
    return value as SchedulerStateItem;
  }
  if (discriminator === "repository_storage") {
    const item = exactObject(value, path, ["kind", "repository_id", "state"]);
    canonicalUuid(item.repository_id, `${path}.repository_id`);
    enumValue(item.state, `${path}.state`, STORAGE_STATES);
    return value as SchedulerStateItem;
  }
  fail(`${path}.kind`, "must be a supported scheduler item kind");
}

function exactObject(
  value: unknown,
  path: string,
  expectedKeys: readonly string[],
): Record<string, unknown> {
  if (!isRecord(value)) {
    fail(path, "must be an object");
  }
  const expected = new Set(expectedKeys);
  for (const key of expectedKeys) {
    if (!Object.prototype.hasOwnProperty.call(value, key)) {
      fail(`${path}.${key}`, "is required");
    }
  }
  for (const key of Object.keys(value)) {
    if (!expected.has(key)) {
      fail(`${path}.${key}`, "is not allowed");
    }
  }
  return value;
}

function readArray(value: unknown, path: string): unknown[] {
  if (!Array.isArray(value)) {
    fail(path, "must be an array");
  }
  return value;
}

function integerBetween(
  value: unknown,
  path: string,
  minimum: number,
  maximum: number,
): number {
  if (
    !Number.isSafeInteger(value) ||
    Number(value) < minimum ||
    Number(value) > maximum
  ) {
    fail(path, `must be an integer between ${minimum} and ${maximum}`);
  }
  return Number(value);
}

function u32(value: unknown, path: string): number {
  return integerBetween(value, path, 0, MAX_U32);
}

function nonNegativeSafeInteger(value: unknown, path: string): number {
  return integerBetween(value, path, 0, MAX_SAFE_INTEGER);
}

function exactInteger(value: unknown, path: string, expected: number): void {
  if (value !== expected) {
    fail(path, `must equal ${expected}`);
  }
}

function exactString(value: unknown, path: string, expected: string): void {
  if (value !== expected) {
    fail(path, `must equal ${expected}`);
  }
}

function enumValue<T extends string>(
  value: unknown,
  path: string,
  allowed: readonly T[],
): T {
  if (typeof value !== "string" || !allowed.includes(value as T)) {
    fail(path, `must be one of ${allowed.join(", ")}`);
  }
  return value as T;
}

function canonicalUuid(value: unknown, path: string): string {
  if (typeof value !== "string" || !UUID.test(value)) {
    fail(path, "must be a canonical lowercase UUID");
  }
  return value;
}

function canonicalUuidV4(value: unknown, path: string): string {
  if (typeof value !== "string" || !UUID_V4.test(value)) {
    fail(path, "must be a canonical lowercase UUID v4");
  }
  return value;
}

function utcTimestamp(value: unknown, path: string): string {
  if (!isValidRfc3339UtcTimestamp(value)) {
    fail(path, "must be an RFC3339 UTC timestamp");
  }
  return value;
}

function lowercaseDigest(value: unknown, path: string): string {
  if (typeof value !== "string" || !DIGEST.test(value)) {
    fail(path, "must be a lowercase SHA-256 digest");
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
