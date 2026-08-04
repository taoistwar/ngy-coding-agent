import { describe, expect, it } from "vitest";

import {
  canonicalizeSchedulerState,
  schedulerStateDigest,
  type SchedulerSnapshotCandidate,
} from "../api/schedulerSnapshot";
import type { SchedulerState } from "../api/types";
import {
  acceptSchedulerCandidate,
  adoptSchedulerBootstrap,
  advanceSchedulerCausalPosition,
  initialSchedulerProjection,
  markSchedulerStale,
} from "./schedulerProjection";

function scheduler(overrides: Partial<SchedulerState> = {}): SchedulerState {
  return {
    schema_version: 1,
    server_instance_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    server_started_at: "2026-07-29T00:00:00Z",
    generation: 2,
    as_of_event_id: 3,
    service_state_generation: 4,
    admission_state: "running",
    limits: {
      global: 2,
      per_repository: 1,
      queued: 32,
      cargo_jobs_per_task: 4,
    },
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
    ...overrides,
  };
}

async function candidate(
  snapshot: SchedulerState,
): Promise<SchedulerSnapshotCandidate> {
  return {
    snapshot,
    canonicalJson: canonicalizeSchedulerState(snapshot),
    digest: await schedulerStateDigest(snapshot),
  };
}

describe("scheduler projection causal arbitration", () => {
  it("adopts an exact Bootstrap snapshot as fresh", () => {
    const snapshot = scheduler();
    const projection = adoptSchedulerBootstrap(
      initialSchedulerProjection,
      snapshot,
      3,
      3,
      4,
    );

    expect(projection).toMatchObject({
      snapshot,
      freshness: "fresh",
      staleReason: null,
      pending: null,
      recoveryReason: null,
    });
  });

  it("does not apply a Bootstrap scheduler whose service watermark is old", () => {
    const projection = adoptSchedulerBootstrap(
      initialSchedulerProjection,
      scheduler(),
      3,
      3,
      5,
    );

    expect(projection).toEqual(initialSchedulerProjection);
  });

  it("caches a complete future candidate and applies it only at both exact watermarks", async () => {
    const snapshot = scheduler({ as_of_event_id: 6, service_state_generation: 5 });
    const pending = acceptSchedulerCandidate(
      initialSchedulerProjection,
      await candidate(snapshot),
      5,
      5,
      4,
    );

    expect(pending).toMatchObject({
      snapshot: null,
      freshness: "unavailable",
      pending: { snapshot },
    });
    expect(advanceSchedulerCausalPosition(pending, 6, 6, 4).snapshot).toBeNull();
    expect(advanceSchedulerCausalPosition(pending, 5, 5, 5).snapshot).toBeNull();
    expect(advanceSchedulerCausalPosition(pending, 6, 6, 5)).toMatchObject({
      snapshot,
      freshness: "fresh",
      pending: null,
    });
  });

  it("drops old candidates when either client watermark is already ahead", async () => {
    const snapshot = scheduler();

    expect(
      acceptSchedulerCandidate(
        initialSchedulerProjection,
        await candidate(snapshot),
        4,
        4,
        4,
      ),
    ).toEqual(initialSchedulerProjection);
    expect(
      acceptSchedulerCandidate(
        initialSchedulerProjection,
        await candidate(snapshot),
        3,
        3,
        5,
      ),
    ).toEqual(initialSchedulerProjection);
  });

  it("clears current and pending state for an incomparable causal tuple", async () => {
    const current = adoptSchedulerBootstrap(
      initialSchedulerProjection,
      scheduler({ generation: 1, as_of_event_id: 2, service_state_generation: 4 }),
      2,
      2,
      4,
    );
    const future = scheduler({ generation: 2, as_of_event_id: 5, service_state_generation: 3 });
    const conflicted = acceptSchedulerCandidate(
      current,
      await candidate(future),
      4,
      4,
      4,
    );

    expect(conflicted).toEqual({
      ...initialSchedulerProjection,
      recoveryReason: "scheduler_causal_tuple_incomparable",
    });
  });

  it("requires recovery when the complete task cursor has passed a pending membership watermark", async () => {
    const snapshot = scheduler({ as_of_event_id: 6 });
    const pending = acceptSchedulerCandidate(
      initialSchedulerProjection,
      await candidate(snapshot),
      5,
      5,
      4,
    );

    expect(advanceSchedulerCausalPosition(pending, 6, 5, 4)).toEqual({
      ...initialSchedulerProjection,
      recoveryReason: "scheduler_event_watermark_impossible",
    });
  });
});

describe("scheduler projection generation arbitration", () => {
  it("ignores lower generations and adopts a higher exact generation", async () => {
    const currentSnapshot = scheduler({ generation: 3 });
    const current = adoptSchedulerBootstrap(
      initialSchedulerProjection,
      currentSnapshot,
      3,
      3,
      4,
    );

    expect(
      acceptSchedulerCandidate(
        current,
        await candidate(scheduler({ generation: 2 })),
        3,
        3,
        4,
      ),
    ).toEqual(current);

    const newer = scheduler({ generation: 4, admission_state: "paused" });
    expect(
      acceptSchedulerCandidate(current, await candidate(newer), 3, 3, 4),
    ).toMatchObject({ snapshot: newer, freshness: "fresh" });
  });

  it("uses an identical replay to clear stale but rejects same-generation payload drift", async () => {
    const snapshot = scheduler();
    const current = markSchedulerStale(
      adoptSchedulerBootstrap(initialSchedulerProjection, snapshot, 3, 3, 4),
      "connection_reconnecting",
    );
    const replayed = acceptSchedulerCandidate(
      current,
      await candidate(snapshot),
      3,
      3,
      4,
    );

    expect(replayed).toMatchObject({ freshness: "fresh", staleReason: null });

    const conflict = acceptSchedulerCandidate(
      replayed,
      await candidate(scheduler({ admission_state: "paused" })),
      3,
      3,
      4,
    );
    expect(conflict).toEqual({
      ...initialSchedulerProjection,
      recoveryReason: "scheduler_generation_conflict",
    });
  });

  it("requires recovery when the server instance changes", async () => {
    const current = adoptSchedulerBootstrap(
      initialSchedulerProjection,
      scheduler(),
      3,
      3,
      4,
    );
    const nextInstance = scheduler({
      server_instance_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      generation: 3,
    });

    expect(
      acceptSchedulerCandidate(current, await candidate(nextInstance), 3, 3, 4),
    ).toEqual({
      ...initialSchedulerProjection,
      recoveryReason: "scheduler_instance_changed",
    });
  });

  it("requires recovery when server_started_at changes inside one epoch", async () => {
    const current = adoptSchedulerBootstrap(
      initialSchedulerProjection,
      scheduler(),
      3,
      3,
      4,
    );
    const drifted = scheduler({
      generation: 3,
      server_started_at: "2026-07-29T00:00:01Z",
    });

    expect(
      acceptSchedulerCandidate(current, await candidate(drifted), 3, 3, 4),
    ).toEqual({
      ...initialSchedulerProjection,
      recoveryReason: "scheduler_started_at_changed",
    });
  });

  it("does not let an older Bootstrap overwrite a higher complete live generation", async () => {
    const bootstrapSnapshot = scheduler({ generation: 2 });
    const liveSnapshot = scheduler({ generation: 3, admission_state: "paused" });
    const live = acceptSchedulerCandidate(
      initialSchedulerProjection,
      await candidate(liveSnapshot),
      3,
      3,
      4,
    );

    const afterOldBootstrap = adoptSchedulerBootstrap(
      live,
      bootstrapSnapshot,
      3,
      3,
      4,
    );

    expect(afterOldBootstrap).toMatchObject({
      snapshot: liveSnapshot,
      freshness: "fresh",
      pending: null,
    });
  });

  it("preserves the first stale boundary without fabricating a snapshot", () => {
    expect(markSchedulerStale(initialSchedulerProjection, "disconnect")).toBe(
      initialSchedulerProjection,
    );
    const fresh = adoptSchedulerBootstrap(
      initialSchedulerProjection,
      scheduler(),
      3,
      3,
      4,
    );
    const stale = markSchedulerStale(fresh, "membership_event_advanced");

    expect(markSchedulerStale(stale, "later_reason")).toBe(stale);
    expect(stale).toMatchObject({
      freshness: "stale",
      staleReason: "membership_event_advanced",
    });
  });
});
