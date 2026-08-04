import { describe, expect, it } from "vitest";

import fixture from "../../../testdata/scheduler-state-rfc8785.json";
import type { SchedulerState } from "./types";
import {
  SchedulerSnapshotAssembler,
  SchedulerSnapshotError,
  canonicalizeSchedulerString,
  canonicalizeSchedulerState,
  schedulerStateDigest,
  type SchedulerStateChunkControl,
  type SchedulerStateControl,
  type SchedulerStateItem,
  validateSchedulerStateChunkControl,
  validateSchedulerStateControl,
} from "./schedulerSnapshot";

const SERVER_INSTANCE_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const SERVER_STARTED_AT = "2026-07-27T00:00:00Z";
const QUEUED_TASK_ID = "11111111-1111-4111-8111-111111111111";
const STOPPING_TASK_ID = "22222222-2222-4222-8222-222222222222";
const REPOSITORY_ID = "33333333-3333-4333-8333-333333333333";

function scheduler(overrides: Partial<SchedulerState> = {}): SchedulerState {
  return {
    schema_version: 1,
    server_instance_id: SERVER_INSTANCE_ID,
    server_started_at: SERVER_STARTED_AT,
    generation: 5,
    as_of_event_id: 9,
    service_state_generation: 3,
    admission_state: "running",
    limits: {
      global: 2,
      per_repository: 1,
      queued: 8,
      cargo_jobs_per_task: 2,
    },
    active_task_count: 1,
    queued_task_count: 1,
    queued_tasks: [{ task_id: QUEUED_TASK_ID, reason: "global_capacity" }],
    stopping_tasks: [
      { task_id: STOPPING_TASK_ID, intent: "user_cancelled" },
    ],
    storage: {
      state: "normal",
      data: { state: "normal" },
      runtime: { state: "normal" },
      repositories: [{ repository_id: REPOSITORY_ID, state: "normal" }],
    },
    ...overrides,
  };
}

function items(value: SchedulerState): SchedulerStateItem[] {
  return [
    ...value.queued_tasks.map((queued) => ({
      kind: "queued_task" as const,
      ...queued,
    })),
    ...value.stopping_tasks.map((stopping) => ({
      kind: "stopping_task" as const,
      ...stopping,
    })),
    ...value.storage.repositories.map((repository) => ({
      kind: "repository_storage" as const,
      ...repository,
    })),
  ];
}

async function manifest(
  value: SchedulerState,
  overrides: Partial<SchedulerStateControl> = {},
): Promise<SchedulerStateControl> {
  const itemCount =
    value.queued_tasks.length +
    value.stopping_tasks.length +
    value.storage.repositories.length;
  return {
    schema_version: 1,
    kind: "scheduler.state",
    server_instance_id: value.server_instance_id,
    server_started_at: value.server_started_at,
    generation: value.generation,
    as_of_event_id: value.as_of_event_id,
    service_state_generation: value.service_state_generation,
    admission_state: value.admission_state,
    limits: value.limits,
    active_task_count: value.active_task_count,
    queued_task_count: value.queued_task_count,
    stopping_task_count: value.stopping_tasks.length,
    repository_storage_count: value.storage.repositories.length,
    storage: {
      state: value.storage.state,
      data: value.storage.data,
      runtime: value.storage.runtime,
    },
    item_count: itemCount,
    chunk_count: Math.ceil(itemCount / 128),
    snapshot_digest: await schedulerStateDigest(value),
    ...overrides,
  };
}

async function chunk(
  value: SchedulerState,
  index = 0,
  overrides: Partial<SchedulerStateChunkControl> = {},
): Promise<SchedulerStateChunkControl> {
  const allItems = items(value);
  return {
    schema_version: 1,
    kind: "scheduler.state.chunk",
    server_instance_id: value.server_instance_id,
    generation: value.generation,
    snapshot_digest: await schedulerStateDigest(value),
    chunk_index: index,
    chunk_count: Math.ceil(allItems.length / 128),
    items: allItems.slice(index * 128, (index + 1) * 128),
    ...overrides,
  };
}

describe("scheduler snapshot wire validation", () => {
  it("accepts exact controls and rejects unknown or missing fields", async () => {
    const value = scheduler();
    const exactManifest = await manifest(value);
    const exactChunk = await chunk(value);

    expect(validateSchedulerStateControl(exactManifest)).toMatchObject({
      kind: "scheduler.state",
      item_count: 3,
      chunk_count: 1,
    });
    expect(validateSchedulerStateChunkControl(exactChunk)).toMatchObject({
      kind: "scheduler.state.chunk",
      chunk_index: 0,
    });
    expect(() =>
      validateSchedulerStateControl({ ...exactManifest, extra: true }),
    ).toThrow(SchedulerSnapshotError);
    expect(() =>
      validateSchedulerStateChunkControl({
        ...exactChunk,
        items: [{ kind: "queued_task", task_id: QUEUED_TASK_ID }],
      }),
    ).toThrow(SchedulerSnapshotError);
  });

  it("rejects inconsistent counts, uppercase digests, and oversized chunks", async () => {
    const value = scheduler();
    const badCount = await manifest(value, { item_count: 2 });
    const uppercaseDigest = await manifest(value, {
      snapshot_digest: "A".repeat(64),
    });
    const oversized = await chunk(value, 0, {
      items: Array.from({ length: 129 }, () => ({
        kind: "queued_task" as const,
        task_id: QUEUED_TASK_ID,
        reason: "global_capacity" as const,
      })),
    });

    expect(() => validateSchedulerStateControl(badCount)).toThrow(/item_count/);
    expect(() => validateSchedulerStateControl(uppercaseDigest)).toThrow(
      /snapshot_digest/,
    );
    expect(() => validateSchedulerStateChunkControl(oversized)).toThrow(/128/);
    const invalidTimestamp = await manifest(value, {
      server_started_at: "2026-99-99T99:99:99Z",
    });
    expect(() =>
      validateSchedulerStateControl(invalidTimestamp),
    ).toThrow(/server_started_at/);
  });
});

describe("restricted RFC 8785 scheduler encoding", () => {
  it("matches the shared Rust bytes, digest, safe integer, and Unicode probe", async () => {
    const value = fixture.snapshot as SchedulerState;

    expect(canonicalizeSchedulerState(value)).toBe(fixture.canonical_json);
    expect(await schedulerStateDigest(value)).toBe(fixture.sha256);
    expect(value.generation).toBe(Number.MAX_SAFE_INTEGER);
    expect(canonicalizeSchedulerString(fixture.unicode_string.source)).toBe(
      fixture.unicode_string.canonical_json,
    );
  });
});

describe("SchedulerSnapshotAssembler", () => {
  it("atomically completes an empty snapshot from its manifest", async () => {
    const value = scheduler({
      active_task_count: 0,
      queued_task_count: 0,
      queued_tasks: [],
      stopping_tasks: [],
      storage: {
        state: "normal",
        data: { state: "normal" },
        runtime: { state: "normal" },
        repositories: [],
      },
    });
    const assembler = new SchedulerSnapshotAssembler();
    const result = await assembler.accept(await manifest(value));

    expect(result).toMatchObject({
      kind: "complete",
      candidate: { snapshot: value, digest: await schedulerStateDigest(value) },
    });
    expect(assembler.hasPartial).toBe(false);
  });

  it("keeps one partial and publishes only after the final ordered chunk", async () => {
    const value = scheduler();
    const assembler = new SchedulerSnapshotAssembler();

    await expect(assembler.accept(await manifest(value))).resolves.toEqual({
      kind: "pending",
    });
    expect(assembler.hasPartial).toBe(true);
    const result = await assembler.accept(await chunk(value));
    expect(result).toMatchObject({
      kind: "complete",
      candidate: { snapshot: value, digest: await schedulerStateDigest(value) },
    });
    expect(assembler.hasPartial).toBe(false);
  });

  it("preempts an older partial and ignores its late chunks", async () => {
    const oldValue = scheduler({ generation: 5 });
    const newValue = scheduler({ generation: 6 });
    const assembler = new SchedulerSnapshotAssembler();

    await assembler.accept(await manifest(oldValue));
    await assembler.accept(await manifest(newValue));
    await expect(assembler.accept(await chunk(oldValue))).resolves.toEqual({
      kind: "ignored",
      reason: "stale_generation",
    });
    await expect(assembler.accept(await chunk(newValue))).resolves.toMatchObject({
      kind: "complete",
      candidate: { snapshot: newValue },
    });
  });

  it("fails closed on missing, duplicate, or out-of-order chunks", async () => {
    const value = schedulerWithQueuedTasks(129);
    const first = await chunk(value, 0);

    await expect(
      new SchedulerSnapshotAssembler().accept(first),
    ).rejects.toThrow(/manifest/);

    const duplicate = new SchedulerSnapshotAssembler();
    await duplicate.accept(await manifest(value));
    await duplicate.accept(first);
    await expect(duplicate.accept(first)).rejects.toThrow(/duplicate/);

    const outOfOrder = new SchedulerSnapshotAssembler();
    await outOfOrder.accept(await manifest(value));
    await expect(outOfOrder.accept(await chunk(value, 1))).rejects.toThrow(
      /out-of-order|missing/,
    );

    const duplicateManifest = new SchedulerSnapshotAssembler();
    const exactManifest = await manifest(value);
    await duplicateManifest.accept(exactManifest);
    await expect(duplicateManifest.accept(exactManifest)).rejects.toThrow(
      /duplicate manifest/,
    );
    expect(duplicateManifest.hasPartial).toBe(false);
  });

  it("fails closed on epoch, order, digest, and same-generation conflicts", async () => {
    const value = scheduler();

    const epoch = new SchedulerSnapshotAssembler();
    await epoch.accept(await manifest(value));
    await expect(
      epoch.accept(
        await chunk(value, 0, {
          server_instance_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        }),
      ),
    ).rejects.toThrow(/server_instance_id/);

    const order = new SchedulerSnapshotAssembler();
    await order.accept(await manifest(value));
    await expect(
      order.accept(await chunk(value, 0, { items: [...items(value)].reverse() })),
    ).rejects.toThrow(/canonical item order/);

    const digest = new SchedulerSnapshotAssembler();
    await digest.accept(
      await manifest(value, { snapshot_digest: "0".repeat(64) }),
    );
    await expect(
      digest.accept(
        await chunk(value, 0, { snapshot_digest: "0".repeat(64) }),
      ),
    ).rejects.toThrow(/digest/);
    expect(digest.hasPartial).toBe(false);

    const conflict = new SchedulerSnapshotAssembler();
    const exactManifest = await manifest(value);
    await conflict.accept(exactManifest);
    await conflict.accept(await chunk(value));
    await expect(
      conflict.accept({ ...exactManifest, admission_state: "paused" }),
    ).rejects.toThrow(
      /same generation/,
    );

    const duplicateChunk = new SchedulerSnapshotAssembler();
    await duplicateChunk.accept(exactManifest);
    const exactChunk = await chunk(value);
    await duplicateChunk.accept(exactChunk);
    await expect(duplicateChunk.accept(exactChunk)).rejects.toThrow(
      /duplicate chunk/,
    );
  });

  it("allows an exact completed generation to replay atomically", async () => {
    const value = scheduler();
    const exactManifest = await manifest(value);
    const exactChunk = await chunk(value);
    const assembler = new SchedulerSnapshotAssembler();

    await assembler.accept(exactManifest);
    await assembler.accept(exactChunk);
    await expect(assembler.accept(exactManifest)).resolves.toEqual({
      kind: "pending",
    });
    await expect(assembler.accept(exactChunk)).resolves.toMatchObject({
      kind: "complete",
      candidate: { snapshot: value },
    });
  });

  it("clears its partial on reset", async () => {
    const value = scheduler();
    const assembler = new SchedulerSnapshotAssembler();
    await assembler.accept(await manifest(value));

    assembler.reset();

    expect(assembler.hasPartial).toBe(false);
    await expect(assembler.accept(await chunk(value))).rejects.toThrow(/manifest/);
  });
});

function schedulerWithQueuedTasks(count: number): SchedulerState {
  return scheduler({
    active_task_count: 0,
    queued_task_count: count,
    queued_tasks: Array.from({ length: count }, (_, index) => ({
      task_id: indexedUuid(index),
      reason: "global_capacity" as const,
    })),
    stopping_tasks: [],
    limits: {
      global: 2,
      per_repository: 1,
      queued: 256,
      cargo_jobs_per_task: 2,
    },
    storage: {
      state: "normal",
      data: { state: "normal" },
      runtime: { state: "normal" },
      repositories: [],
    },
  });
}

function indexedUuid(index: number): string {
  return `44444444-4444-4444-8444-${index.toString(16).padStart(12, "0")}`;
}
