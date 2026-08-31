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
    act(() => {
      result.current.refresh();
      result.current.refresh();
    });
    expect(taskDelivery).toHaveBeenCalledTimes(1);

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
    expect(taskDelivery).toHaveBeenCalledTimes(2);
    expect(clock.delays).toEqual([]);
  });

  it("coalesces same-task refreshes into one trailing delivery load", async () => {
    const first = deferred<DeliveryTask>();
    const second = deferred<DeliveryTask>();
    const refreshed = projection(TASK_A);
    refreshed.target = {
      available: true,
      branch: "refs/heads/main",
      head: OID_B,
    };
    const signals: AbortSignal[] = [];
    const taskDelivery = vi.fn(
      async (_taskId: string, signal?: AbortSignal): Promise<DeliveryTask> => {
        if (signal !== undefined) signals.push(signal);
        return taskDelivery.mock.calls.length === 1 ? first.promise : second.promise;
      },
    );
    const api: DeliveryPollingApi = {
      taskDelivery,
      deliveryOperation: vi.fn<
        (operationId: string, signal?: AbortSignal) => Promise<DeliveryOperation>
      >(),
    };
    const { result } = renderHook(() => useDeliveryPolling({ api, taskId: TASK_A }));
    await waitFor(() => expect(taskDelivery).toHaveBeenCalledTimes(1));

    act(() => {
      result.current.refresh();
      result.current.refresh();
      result.current.refresh();
    });
    expect(taskDelivery).toHaveBeenCalledTimes(1);
    expect(signals[0]?.aborted).toBe(false);

    await act(async () => {
      first.resolve(projection(TASK_A));
      await first.promise;
    });
    await waitFor(() => expect(taskDelivery).toHaveBeenCalledTimes(2));
    expect(signals[0]?.aborted).toBe(false);
    expect(signals[1]?.aborted).toBe(false);

    await act(async () => {
      second.resolve(refreshed);
      await second.promise;
    });
    await waitFor(() => expect(result.current.state.phase).toBe("ready"));
    expect(result.current.state.projection).toEqual(refreshed);
    expect(taskDelivery).toHaveBeenCalledTimes(2);
  });

  it("runs one queued refresh after a delivery load fails", async () => {
    const first = deferred<DeliveryTask>();
    const refreshed = projection(TASK_A);
    refreshed.target = {
      available: true,
      branch: "refs/heads/main",
      head: OID_B,
    };
    const taskDelivery = vi
      .fn<(taskId: string, signal?: AbortSignal) => Promise<DeliveryTask>>()
      .mockImplementationOnce(() => first.promise)
      .mockResolvedValueOnce(refreshed);
    const api: DeliveryPollingApi = {
      taskDelivery,
      deliveryOperation: vi.fn<
        (operationId: string, signal?: AbortSignal) => Promise<DeliveryOperation>
      >(),
    };
    const { result } = renderHook(() => useDeliveryPolling({ api, taskId: TASK_A }));
    await waitFor(() => expect(taskDelivery).toHaveBeenCalledTimes(1));
    act(() => {
      result.current.refresh();
      result.current.refresh();
    });

    await act(async () => {
      first.reject({
        code: "DELIVERY_REQUEST_FAILED",
        message: "delivery request failed",
        retryable: true,
      });
      await Promise.resolve();
    });
    await waitFor(() => expect(taskDelivery).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.state.phase).toBe("ready"));
    expect(result.current.state.projection).toEqual(refreshed);
    expect(taskDelivery).toHaveBeenCalledTimes(2);
  });

  it("clears a queued same-task refresh when its polling session is disposed", async () => {
    const pending = deferred<DeliveryTask>();
    let signal: AbortSignal | undefined;
    const taskDelivery = vi.fn(async (_taskId: string, nextSignal?: AbortSignal) => {
      signal = nextSignal;
      return pending.promise;
    });
    const api: DeliveryPollingApi = {
      taskDelivery,
      deliveryOperation: vi.fn<
        (operationId: string, signal?: AbortSignal) => Promise<DeliveryOperation>
      >(),
    };
    const { result, unmount } = renderHook(() =>
      useDeliveryPolling({ api, taskId: TASK_A }),
    );
    await waitFor(() => expect(taskDelivery).toHaveBeenCalledTimes(1));
    act(() => result.current.refresh());

    unmount();
    expect(signal?.aborted).toBe(true);
    pending.resolve(projection(TASK_A));
    await pending.promise;
    expect(taskDelivery).toHaveBeenCalledTimes(1);
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

  it("queues refresh behind an operation poll without aborting it", async () => {
    const clock = new ManualClock();
    const pendingOperation = deferred<DeliveryOperation>();
    const refreshed = projection(TASK_A, operation(3, "merged"));
    const taskDelivery = vi
      .fn<(taskId: string, signal?: AbortSignal) => Promise<DeliveryTask>>()
      .mockResolvedValueOnce(projection(TASK_A, operation(1)))
      .mockResolvedValueOnce(refreshed);
    let operationSignal: AbortSignal | undefined;
    const deliveryOperation = vi.fn(async (_operationId: string, signal?: AbortSignal) => {
      operationSignal = signal;
      return pendingOperation.promise;
    });
    const api: DeliveryPollingApi = { taskDelivery, deliveryOperation };
    const { result } = renderHook(() =>
      useDeliveryPolling({ api, taskId: TASK_A, clock }),
    );
    await waitFor(() => expect(clock.delays).toEqual([500]));
    act(() => result.current.refresh());
    expect(taskDelivery).toHaveBeenCalledTimes(1);
    expect(clock.delays).toEqual([500]);

    await act(async () => {
      clock.fireNext();
      await Promise.resolve();
    });
    await waitFor(() => expect(operationSignal).toBeDefined());
    act(() => {
      result.current.refresh();
      result.current.refresh();
    });
    expect(operationSignal?.aborted).toBe(false);
    expect(taskDelivery).toHaveBeenCalledTimes(1);

    await act(async () => {
      pendingOperation.resolve(operation(2, "merge_pending"));
      await pendingOperation.promise;
    });
    await waitFor(() => expect(taskDelivery).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.state.phase).toBe("ready"));
    expect(result.current.state.projection).toEqual(refreshed);
    expect(taskDelivery).toHaveBeenCalledTimes(2);
    expect(clock.delays).toEqual([]);
  });

  it.each<{
    name: string;
    initial: DeliveryMergeOperationEnvelope;
    outcome:
      | { kind: "resolve"; value: DeliveryOperation }
      | { kind: "reject"; retryable: boolean };
  }>([
    {
      name: "a stale operation response",
      initial: operation(2, "merge_pending"),
      outcome: { kind: "resolve", value: operation(1) },
    },
    {
      name: "a mismatched operation response",
      initial: operation(1),
      outcome: {
        kind: "resolve",
        value: { ...operation(2), operation_id: CLEANUP_ID },
      },
    },
    {
      name: "a retryable operation error",
      initial: operation(1),
      outcome: { kind: "reject", retryable: true },
    },
    {
      name: "a non-retryable operation error",
      initial: operation(1),
      outcome: { kind: "reject", retryable: false },
    },
  ])("runs one queued refresh after $name", async ({ initial, outcome }) => {
    const clock = new ManualClock();
    const pendingOperation = deferred<DeliveryOperation>();
    const refreshed = projection(TASK_A, operation(3, "merged"));
    const taskDelivery = vi
      .fn<(taskId: string, signal?: AbortSignal) => Promise<DeliveryTask>>()
      .mockResolvedValueOnce(projection(TASK_A, initial))
      .mockResolvedValueOnce(refreshed);
    let operationSignal: AbortSignal | undefined;
    const deliveryOperation = vi.fn(async (_operationId: string, signal?: AbortSignal) => {
      operationSignal = signal;
      return pendingOperation.promise;
    });
    const api: DeliveryPollingApi = { taskDelivery, deliveryOperation };
    const { result } = renderHook(() =>
      useDeliveryPolling({ api, taskId: TASK_A, clock }),
    );
    await waitFor(() => expect(clock.delays).toEqual([500]));
    await act(async () => {
      clock.fireNext();
      await Promise.resolve();
    });
    await waitFor(() => expect(operationSignal).toBeDefined());
    act(() => {
      result.current.refresh();
      result.current.refresh();
    });
    expect(operationSignal?.aborted).toBe(false);

    await act(async () => {
      if (outcome.kind === "resolve") {
        pendingOperation.resolve(outcome.value);
      } else {
        pendingOperation.reject({
          code: "DELIVERY_OPERATION_FAILED",
          message: "operation request failed",
          retryable: outcome.retryable,
        });
      }
      await Promise.resolve();
    });
    await waitFor(() => expect(taskDelivery).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.state.phase).toBe("ready"));
    expect(operationSignal?.aborted).toBe(false);
    expect(result.current.state.projection).toEqual(refreshed);
    expect(taskDelivery).toHaveBeenCalledTimes(2);
    expect(clock.delays).toEqual([]);
  });

  it("clears a queued refresh when a newer operation is tracked", async () => {
    const clock = new ManualClock();
    const firstPoll = deferred<DeliveryOperation>();
    const secondPoll = deferred<DeliveryOperation>();
    const taskDelivery = vi.fn(async () => projection(TASK_A, operation(1)));
    const operationSignals: AbortSignal[] = [];
    const deliveryOperation = vi.fn(
      async (_operationId: string, signal?: AbortSignal) => {
        if (signal !== undefined) operationSignals.push(signal);
        return deliveryOperation.mock.calls.length === 1
          ? firstPoll.promise
          : secondPoll.promise;
      },
    );
    const api: DeliveryPollingApi = { taskDelivery, deliveryOperation };
    const { result } = renderHook(() =>
      useDeliveryPolling({ api, taskId: TASK_A, clock }),
    );
    await waitFor(() => expect(clock.delays).toEqual([500]));
    await act(async () => {
      clock.fireNext();
      await Promise.resolve();
    });
    await waitFor(() => expect(deliveryOperation).toHaveBeenCalledTimes(1));
    act(() => result.current.refresh());
    act(() => result.current.trackOperation(operation(2, "merge_pending")));
    expect(operationSignals[0]?.aborted).toBe(true);
    expect(clock.delays).toEqual([500]);

    firstPoll.resolve(operation(3, "merge_pending"));
    await firstPoll.promise;
    await act(async () => {
      clock.fireNext();
      await Promise.resolve();
    });
    await waitFor(() => expect(deliveryOperation).toHaveBeenCalledTimes(2));
    await act(async () => {
      secondPoll.resolve(operation(3, "merge_pending"));
      await secondPoll.promise;
    });
    await waitFor(() => expect(clock.delays).toEqual([500]));
    expect(taskDelivery).toHaveBeenCalledTimes(1);
    expect(result.current.state.operation?.version).toBe(3);
  });
});
