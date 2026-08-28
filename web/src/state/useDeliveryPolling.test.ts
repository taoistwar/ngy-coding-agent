import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type {
  DeliveryCleanupOperationEnvelope,
  DeliveryMergeOperationEnvelope,
  DeliveryOperation,
  DeliveryTask,
} from "../api/types";
import {
  useDeliveryPolling,
  type DeliveryPollingApi,
  type DeliveryPollingClock,
} from "./useDeliveryPolling";

const TASK_A = "11111111-1111-4111-8111-111111111111";
const TASK_B = "22222222-2222-4222-8222-222222222222";
const OPERATION_ID = "33333333-3333-4333-8333-333333333333";
const CLEANUP_ID = "44444444-4444-4444-8444-444444444444";
const OID_A = "1".repeat(40);
const OID_B = "2".repeat(40);
const FINGERPRINT = "a".repeat(64);

function operation(
  version: number,
  state: DeliveryMergeOperationEnvelope["state"] = "accepted",
): DeliveryMergeOperationEnvelope {
  return {
    kind: "merge",
    operation_id: OPERATION_ID,
    version,
    state,
    review_generation: 7,
    workspace_fingerprint: FINGERPRINT,
    candidate_source_tree: OID_B,
    preflight_source_commit: OID_B,
    source_commit:
      state === "merge_pending" || state === "merged" ? OID_B : null,
    target_branch: "refs/heads/main",
    target_head: OID_A,
    conflicts: null,
    failure: null,
  };
}

function cleanupOperation(
  version: number,
  state: DeliveryCleanupOperationEnvelope["state"] = "remove_pending",
): DeliveryCleanupOperationEnvelope {
  return {
    kind: "cleanup",
    operation_id: CLEANUP_ID,
    cleanup_kind: "remove_worktree",
    version,
    state,
    expected_disposition_version: 1,
    expected_merge_operation_id: OPERATION_ID,
    expected_source_ref: "refs/heads/coding-agent/task",
    expected_source_oid: OID_B,
    target_branch: null,
    target_head: null,
    failure: null,
  };
}

function projection(
  taskId: string,
  latest: DeliveryMergeOperationEnvelope | null = null,
): DeliveryTask {
  let latestMerge: DeliveryTask["latest_merge"] = null;
  if (latest !== null) {
    const { kind: _kind, ...payload } = latest;
    latestMerge = payload;
  }
  return {
    task_id: taskId,
    eligibility: "eligible",
    reasons: [],
    evidence: { review_generation: 7, workspace_fingerprint: FINGERPRINT },
    target: { available: true, branch: "refs/heads/main", head: OID_A },
    source: null,
    latest_merge: latestMerge,
    latest_cleanup: null,
    disposition: null,
    allowed_actions: latest === null ? ["run_preflight"] : [],
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

class ManualClock implements DeliveryPollingClock {
  readonly #timers: Array<{ callback: () => void; delayMs: number }> = [];

  setTimeout(callback: () => void, delayMs: number): unknown {
    const timer = { callback, delayMs };
    this.#timers.push(timer);
    return timer;
  }

  clearTimeout(handle: unknown): void {
    const index = this.#timers.indexOf(
      handle as { callback: () => void; delayMs: number },
    );
    if (index >= 0) this.#timers.splice(index, 1);
  }

  get delays(): number[] {
    return this.#timers.map(({ delayMs }) => delayMs);
  }

  fireNext(): void {
    const timer = this.#timers.shift();
    if (timer === undefined) throw new Error("no pending delivery poll timer");
    timer.callback();
  }
}

describe("useDeliveryPolling", () => {
  it("GETs on selection, aborts the old task, and ignores its late response", async () => {
    const first = deferred<DeliveryTask>();
    const second = deferred<DeliveryTask>();
    const signals: AbortSignal[] = [];
    const taskDelivery = vi.fn(
      async (taskId: string, signal?: AbortSignal): Promise<DeliveryTask> => {
        if (signal !== undefined) signals.push(signal);
        return taskId === TASK_A ? first.promise : second.promise;
      },
    );
    const api: DeliveryPollingApi = {
      taskDelivery,
      deliveryOperation: vi.fn<
        (operationId: string, signal?: AbortSignal) => Promise<DeliveryOperation>
      >(),
    };
    const clock = new ManualClock();
    const { result, rerender } = renderHook(
      ({ taskId }: { taskId: string | null }) =>
        useDeliveryPolling({ api, taskId, clock }),
      { initialProps: { taskId: TASK_A } },
    );
    await waitFor(() => expect(taskDelivery).toHaveBeenCalledTimes(1));
    const staleTrackOperation = result.current.trackOperation;

    rerender({ taskId: TASK_B });
    await waitFor(() => expect(taskDelivery).toHaveBeenCalledTimes(2));
    expect(signals[0]?.aborted).toBe(true);

    act(() => {
      first.resolve(projection(TASK_A));
      second.resolve(projection(TASK_B));
    });
    await waitFor(() => expect(result.current.state.projection?.task_id).toBe(TASK_B));
    expect(result.current.state.taskId).toBe(TASK_B);
    expect(result.current.state.operation).toBeNull();
    act(() => staleTrackOperation(operation(1)));
    expect(result.current.state.operation).toBeNull();
    expect(clock.delays).toEqual([]);
  });

  it("backs off 500ms to 2s, resets on version progress, then stops and refreshes", async () => {
    const clock = new ManualClock();
    const taskDelivery = vi
      .fn<(taskId: string, signal?: AbortSignal) => Promise<DeliveryTask>>()
      .mockResolvedValueOnce(projection(TASK_A, operation(1)))
      .mockResolvedValueOnce(projection(TASK_A, operation(3, "merged")));
    const deliveryOperation = vi
      .fn<
        (operationId: string, signal?: AbortSignal) => Promise<DeliveryOperation>
      >()
      .mockResolvedValueOnce(operation(1))
      .mockResolvedValueOnce(operation(1))
      .mockResolvedValueOnce(operation(2, "merge_pending"))
      .mockResolvedValueOnce(operation(3, "merged"));
    const api: DeliveryPollingApi = { taskDelivery, deliveryOperation };
    const { result } = renderHook(() =>
      useDeliveryPolling({ api, taskId: TASK_A, clock }),
    );

    await waitFor(() => expect(clock.delays).toEqual([500]));
    await act(async () => {
      clock.fireNext();
      await Promise.resolve();
    });
    await waitFor(() => expect(clock.delays).toEqual([1_000]));
    await act(async () => {
      clock.fireNext();
      await Promise.resolve();
    });
    await waitFor(() => expect(clock.delays).toEqual([2_000]));
    await act(async () => {
      clock.fireNext();
      await Promise.resolve();
    });
    await waitFor(() => expect(clock.delays).toEqual([500]));
    expect(result.current.state.operation?.version).toBe(2);
    await act(async () => {
      clock.fireNext();
      await Promise.resolve();
    });

    await waitFor(() => expect(taskDelivery).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.state.phase).toBe("ready"));
    expect(result.current.state.operation).toMatchObject({
      operation_id: OPERATION_ID,
      version: 3,
      state: "merged",
    });
    expect(clock.delays).toEqual([]);
  });

  it("resumes a durable cleanup after reload and refreshes the task at terminal", async () => {
    const clock = new ManualClock();
    const pending = cleanupOperation(4);
    const completed = cleanupOperation(5, "completed");
    const initial = projection(TASK_A, operation(9, "merged"));
    const refreshed = projection(TASK_A, operation(9, "merged"));
    const { kind: _pendingKind, ...pendingPayload } = pending;
    const { kind: _completedKind, ...completedPayload } = completed;
    initial.latest_cleanup = pendingPayload;
    refreshed.latest_cleanup = completedPayload;
    const taskDelivery = vi
      .fn<(taskId: string, signal?: AbortSignal) => Promise<DeliveryTask>>()
      .mockResolvedValueOnce(initial)
      .mockResolvedValueOnce(refreshed);
    const deliveryOperation = vi
      .fn<
        (operationId: string, signal?: AbortSignal) => Promise<DeliveryOperation>
      >()
      .mockResolvedValueOnce(completed);
    const api: DeliveryPollingApi = { taskDelivery, deliveryOperation };
    const { result } = renderHook(() =>
      useDeliveryPolling({
        api,
        taskId: TASK_A,
        clock,
      }),
    );

    await waitFor(() => expect(clock.delays).toEqual([500]));
    expect(result.current.state.operation).toMatchObject({
      kind: "cleanup",
      operation_id: CLEANUP_ID,
      version: 4,
    });
    await act(async () => {
      clock.fireNext();
      await Promise.resolve();
    });

    await waitFor(() => expect(deliveryOperation).toHaveBeenCalledWith(
      CLEANUP_ID,
      expect.any(AbortSignal),
    ));
    await waitFor(() => expect(taskDelivery).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.state.phase).toBe("ready"));
    expect(result.current.state.operation).toMatchObject({
      kind: "cleanup",
      operation_id: CLEANUP_ID,
      version: 5,
      state: "completed",
    });
    expect(clock.delays).toEqual([]);
  });

  it("polls a durable command result and aborts its request on unmount", async () => {
    const clock = new ManualClock();
    const pending = deferred<DeliveryOperation>();
    let operationSignal: AbortSignal | undefined;
    const api: DeliveryPollingApi = {
      taskDelivery: vi.fn(async () => projection(TASK_A)),
      deliveryOperation: vi.fn(async (_operationId, signal) => {
        operationSignal = signal;
        return pending.promise;
      }),
    };
    const { result, unmount } = renderHook(() =>
      useDeliveryPolling({ api, taskId: TASK_A, clock }),
    );
    await waitFor(() => expect(result.current.state.phase).toBe("ready"));

    act(() => result.current.trackOperation(operation(1)));
    expect(clock.delays).toEqual([500]);
    await act(async () => {
      clock.fireNext();
      await Promise.resolve();
    });
    await waitFor(() => expect(operationSignal).toBeDefined());

    unmount();
    expect(operationSignal?.aborted).toBe(true);
    pending.resolve(operation(2, "merge_pending"));
  });
});
