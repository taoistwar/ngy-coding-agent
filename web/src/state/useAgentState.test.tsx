import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import {
  schedulerStateDigest,
  type SchedulerSnapshotCandidate,
} from "../api/schedulerSnapshot";
import type {
  BootstrapResponse,
  CancellationAcceptedResponse,
  PlanSnapshot,
  Repository,
  ReviewEvidence,
  Task,
  TaskDetail,
  TaskEvent,
} from "../api/types";
import {
  type AgentStreamCallbacks,
  createSseAgentStreamFactory,
  type UseAgentStateDependencies,
  useAgentState,
} from "./useAgentState";

const NOW = "2026-07-15T00:00:00Z";
const STREAM_REVIEW_TASK_ID = "00000000-0000-4000-8000-000000000001";
const SCHEDULER_REPOSITORY_ID = "00000000-0000-4000-8000-000000000002";

function repository(id = SCHEDULER_REPOSITORY_ID): Repository {
  return {
    id,
    display_name: id,
    selected_path: `C:/${id}`,
    git_root: `C:/${id}`,
    cargo_workspace_root: `C:/${id}`,
    created_at: NOW,
    last_opened_at: NOW,
  };
}

function task(id: string, status: Task["status"] = "running", cursor = 10): Task {
  return {
    id,
    repository_id: SCHEDULER_REPOSITORY_ID,
    client_request_id: `request-${id}`,
    prompt: id,
    status,
    delivery_readiness: "unreviewed",
    attempt: 1,
    last_event_id: cursor,
    created_at: NOW,
    retry_of: null,
    started_at: null,
    finished_at: null,
    failure: null,
  };
}

function bootstrap(): BootstrapResponse {
  return {
    csrf_token: "csrf",
    latest_event_id: 10,
    max_concurrent_tasks: 4,
    repositories: [repository()],
    server_started_at: NOW,
    service_state: "ready",
    service_state_generation: 1,
    tasks: [task("task-1"), task("task-2")],
    scheduler: {
      schema_version: 1,
      server_instance_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      server_started_at: NOW,
      generation: 1,
      as_of_event_id: 10,
      service_state_generation: 1,
      admission_state: "running",
      limits: {
        global: 4,
        per_repository: 2,
        queued: 32,
        cargo_jobs_per_task: 2,
      },
      active_task_count: 2,
      queued_task_count: 0,
      queued_tasks: [],
      stopping_tasks: [],
      storage: {
        state: "normal",
        data: { state: "normal" },
        runtime: { state: "normal" },
        repositories: [
          { repository_id: SCHEDULER_REPOSITORY_ID, state: "normal" },
        ],
      },
    },
  };
}

function schedulerCandidate(
  overrides: Partial<BootstrapResponse["scheduler"]> = {},
): SchedulerSnapshotCandidate {
  const snapshot = {
    ...structuredClone(bootstrap().scheduler),
    generation: 2,
    ...overrides,
  };
  return {
    snapshot,
    digest: "0".repeat(64),
    canonicalJson: JSON.stringify(snapshot),
  };
}

function detail(id: string, cursor = 10): TaskDetail {
  return {
    task: task(id, "running", cursor),
    event_cursor: cursor,
    plan: null,
    activity: [],
    diff: null,
    tests: null,
    timeline: [],
    reviews: [],
  };
}

function reviewPlan(): PlanSnapshot {
  return {
    format_version: 1,
    revision: 1,
    summary: "Implement and verify the requested change.",
    items: [
      {
        id: "plan-item-1",
        title: "Implement",
        description: "Implement the requested change.",
        acceptance_criteria: ["The required test passes."],
        status: "running",
      },
    ],
    initial_required_checks: [
      {
        id: "check-cargo-test",
        kind: "cargo_test",
        package: null,
        integration_test: null,
      },
    ],
  };
}

function reviewEvidence(
  round: number,
  verdict: ReviewEvidence["verdict"] = "approved",
): ReviewEvidence {
  const workspaceDigest = {
    algorithm: "workspace_fingerprint_v1" as const,
    value: "a".repeat(64),
  };
  const requiredCheck = {
    id: "check-cargo-test",
    kind: "cargo_test" as const,
    package: null,
    integration_test: null,
  };
  return {
    round,
    decision_source: "reviewer",
    workspace_generation: round,
    workspace_digest: workspaceDigest,
    verdict,
    summary:
      verdict === "approved"
        ? `Approved in round ${round}`
        : "A blocking issue remains.",
    findings:
      verdict === "approved"
        ? []
        : [
            {
              id: `review-${round}-finding-1`,
              severity: "blocking",
              message: "Fix the blocking issue.",
              path: "src/lib.rs",
              line: 7,
            },
          ],
    added_required_checks: [],
    required_checks: [requiredCheck],
    check_evidence: [
      {
        check_id: requiredCheck.id,
        actor: "executor",
        role_run: round,
        workspace_generation: round,
        workspace_digest: workspaceDigest,
        status: verdict === "approved" ? "passed" : "failed",
        duration_ms: 12,
        summary: verdict === "approved" ? "passed" : "failed",
        truncated: false,
      },
    ],
    coverage: {
      generation: round,
      workspace_digest: workspaceDigest,
      manifest_sha256: "b".repeat(64),
      covered_chunks: [],
      total_chunks: 0,
    },
    created_at: NOW,
  };
}

function detailWithReviews(
  id: string,
  reviews: ReviewEvidence[],
  cursor = 10,
): TaskDetail {
  return {
    ...detail(id, cursor),
    plan: reviewPlan(),
    reviews,
  };
}

function reviewEvent(
  id: number,
  taskId: string,
  review: ReviewEvidence,
): Extract<TaskEvent, { kind: "review.updated" }> {
  return {
    id,
    schema_version: 1,
    task_id: taskId,
    kind: "review.updated",
    created_at: NOW,
    payload: { review },
  };
}

function activityEvent(id: number, taskId: string): TaskEvent {
  return {
    id,
    schema_version: 1,
    task_id: taskId,
    kind: "activity.appended",
    created_at: NOW,
    payload: {
      entry: {
        id: `activity-${id}`,
        level: "info",
        actor: "system",
        role_run: null,
        message: "Later buffered event.",
        created_at: NOW,
      },
    },
  };
}

function completedEvent(id: number, taskId: string): TaskEvent {
  return {
    id,
    schema_version: 1,
    task_id: taskId,
    kind: "task.completed",
    created_at: NOW,
    payload: { task: task(taskId, "completed", id) },
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

function fixture(detailImpl: (taskId: string) => Promise<TaskDetail>) {
  let callbacks: AgentStreamCallbacks | undefined;
  const start = vi.fn<(_: number) => void>();
  const stop = vi.fn();
  const requestRecovery = vi.fn<(_: string) => void>();
  const initialize = vi.fn(async () => bootstrap());
  const bootstrapRequest = vi.fn<
    (signal?: AbortSignal) => Promise<BootstrapResponse>
  >(async () => bootstrap());
  const taskDetail = vi.fn(detailImpl);
  const cancelTask = vi.fn<(_: string) => Promise<CancellationAcceptedResponse>>(
    async (taskId) => ({
      cancellation_requested: true,
      task: task(taskId),
    }),
  );
  const addRepository = vi.fn(async (path: string) => ({
    ...repository("repo-added"),
    selected_path: path,
  }));
  const pickRepository = vi.fn(async () => repository("repo-picked"));
  const createTaskExecute = vi.fn(async () => task("task-created", "queued", 11));
  const newCreateTask = vi.fn((repositoryId: string, prompt: string) => ({
    clientRequestId: "stable-create-id",
    execute: createTaskExecute,
  }));
  const retryTask = vi.fn(async () => task("task-retry", "queued", 11));
  const quit = vi.fn(async () => ({ status: "shutting_down" as const }));
  const dependencies: UseAgentStateDependencies = {
    api: {
      initialize,
      bootstrap: bootstrapRequest,
      addRepository,
      pickRepository,
      newCreateTask,
      taskDetail,
      cancelTask,
      retryTask,
      quit,
    },
    createStream: (receivedCallbacks) => {
      callbacks = receivedCallbacks;
      return { start, stop, requestRecovery };
    },
  };
  return {
    dependencies,
    get callbacks() {
      if (callbacks === undefined) {
        throw new Error("stream not created");
      }
      return callbacks;
    },
    start,
    stop,
    requestRecovery,
    initialize,
    bootstrapRequest,
    taskDetail,
    cancelTask,
    addRepository,
    pickRepository,
    createTaskExecute,
    newCreateTask,
    retryTask,
    quit,
  };
}

function sessionExpiredError() {
  return {
    status: 401,
    code: "SESSION_EXPIRED",
    message: "reopen",
    retryable: false,
  };
}

describe("useAgentState", () => {
  it("starts SSE as soon as bootstrap completes without waiting for detail", async () => {
    const pendingDetail = deferred<TaskDetail>();
    const testFixture = fixture(() => pendingDetail.promise);
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));

    await waitFor(() => expect(testFixture.start).toHaveBeenCalledWith(10));
    expect(result.current.state.connection).toBe("live");
    expect(testFixture.dependencies.api.taskDetail).not.toHaveBeenCalled();

    act(() => result.current.selectTask("task-1"));
    await waitFor(() =>
      expect(testFixture.dependencies.api.taskDetail).toHaveBeenCalledWith("task-1"),
    );
    expect(testFixture.start).toHaveBeenCalledTimes(1);
  });

  it("installs a complete scheduler candidate forwarded by the stream", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalledWith(10));
    const candidate = schedulerCandidate();

    act(() => {
      testFixture.callbacks.onSchedulerSnapshot(candidate);
    });

    expect(result.current.state.scheduler).toMatchObject({
      snapshot: candidate.snapshot,
      digest: candidate.digest,
      freshness: "fresh",
      recoveryReason: null,
    });
  });

  it("rejects a new scheduler epoch synchronously for transport recovery", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalledWith(10));

    let thrown: unknown;
    act(() => {
      try {
        testFixture.callbacks.onSchedulerSnapshot(
          schedulerCandidate({
            server_instance_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
          }),
        );
      } catch (error) {
        thrown = error;
      }
    });

    expect(thrown).toEqual(
      expect.objectContaining({ name: "SchedulerProjectionProtocolError" }),
    );
    expect(result.current.state.scheduler).toMatchObject({
      snapshot: null,
      recoveryReason: "scheduler_instance_changed",
    });
    expect(result.current.state.connection).toBe("protocol_error");
  });

  it("atomically adopts lower service and scheduler generations from a recovered epoch", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    const oldEpoch = {
      ...bootstrap(),
      service_state_generation: 100,
      scheduler: {
        ...bootstrap().scheduler,
        generation: 100,
        service_state_generation: 100,
      },
    };
    testFixture.initialize.mockResolvedValueOnce(oldEpoch);
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalledWith(10));

    const nextInstanceId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    let epochError: unknown;
    act(() => {
      try {
        testFixture.callbacks.onSchedulerSnapshot(
          schedulerCandidate({
            server_instance_id: nextInstanceId,
            generation: 1,
            service_state_generation: 1,
          }),
        );
      } catch (error) {
        epochError = error;
      }
    });
    expect(epochError).toEqual(
      expect.objectContaining({ name: "SchedulerProjectionProtocolError" }),
    );

    const recoveredEpoch = {
      ...bootstrap(),
      service_state_generation: 1,
      scheduler: {
        ...bootstrap().scheduler,
        server_instance_id: nextInstanceId,
        generation: 1,
        service_state_generation: 1,
      },
    };
    let cursor: number | undefined;
    act(() => {
      cursor = testFixture.callbacks.onBootstrap(recoveredEpoch);
    });

    expect(cursor).toBe(10);
    expect(result.current.state.serviceGeneration).toBe(1);
    expect(result.current.state.serviceState).toBe("ready");
    expect(result.current.state.scheduler).toMatchObject({
      snapshot: recoveredEpoch.scheduler,
      freshness: "fresh",
      pending: null,
      recoveryReason: null,
    });
    expect(result.current.state.connection).toBe("live");
  });

  it("rejects a pending scheduler tuple when a live service update crosses it", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalledWith(10));
    const candidate = schedulerCandidate({
      as_of_event_id: 11,
      service_state_generation: 1,
    });

    act(() => {
      testFixture.callbacks.onSchedulerSnapshot(candidate);
    });
    expect(result.current.state.scheduler.pending?.snapshot).toBe(
      candidate.snapshot,
    );

    let thrown: unknown;
    act(() => {
      try {
        testFixture.callbacks.onServiceState("ready", 2);
      } catch (error) {
        thrown = error;
      }
    });

    expect(thrown).toEqual(
      expect.objectContaining({ name: "SchedulerProjectionProtocolError" }),
    );
    expect(result.current.state.scheduler.recoveryReason).toBe(
      "scheduler_causal_tuple_incomparable",
    );
  });

  it("rejects a pending scheduler tuple when a live membership event crosses it", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalledWith(10));

    act(() => {
      testFixture.callbacks.onSchedulerSnapshot(
        schedulerCandidate({
          as_of_event_id: 10,
          service_state_generation: 2,
        }),
      );
    });

    let thrown: unknown;
    act(() => {
      try {
        testFixture.callbacks.onTaskEvent(completedEvent(11, "task-1"));
      } catch (error) {
        thrown = error;
      }
    });

    expect(thrown).toEqual(
      expect.objectContaining({ name: "SchedulerProjectionProtocolError" }),
    );
    expect(result.current.state.scheduler.recoveryReason).toBe(
      "scheduler_causal_tuple_incomparable",
    );
  });

  it("rejects an unknown persisted event that passes a pending membership watermark", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalledWith(10));

    act(() => {
      testFixture.callbacks.onSchedulerSnapshot(
        schedulerCandidate({ as_of_event_id: 11 }),
      );
    });

    let thrown: unknown;
    act(() => {
      try {
        testFixture.callbacks.onUnknownEvent(11, "future.event", 1);
      } catch (error) {
        thrown = error;
      }
    });

    expect(thrown).toEqual(
      expect.objectContaining({ name: "SchedulerProjectionProtocolError" }),
    );
    expect(result.current.state.scheduler.recoveryReason).toBe(
      "scheduler_event_watermark_impossible",
    );
  });

  it("lets an authoritative new-epoch Bootstrap replace a buffered old-epoch candidate", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalledWith(10));

    act(() => {
      testFixture.callbacks.onSchedulerSnapshot(
        schedulerCandidate({ as_of_event_id: 11 }),
      );
    });

    const nextEpoch = {
      ...bootstrap(),
      scheduler: {
        ...bootstrap().scheduler,
        server_instance_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      },
    };
    let returnedCursor: number | undefined;
    act(() => {
      returnedCursor = testFixture.callbacks.onBootstrap(nextEpoch);
    });

    expect(returnedCursor).toBe(10);
    expect(result.current.state.scheduler).toMatchObject({
      snapshot: nextEpoch.scheduler,
      freshness: "fresh",
      pending: null,
      recoveryReason: null,
    });
  });

  it("buffers live events during detail fetch and replays only ids above its cursor", async () => {
    const pendingDetail = deferred<TaskDetail>();
    const testFixture = fixture(() => pendingDetail.promise);
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalled());

    act(() => result.current.selectTask("task-1"));
    await waitFor(() =>
      expect(testFixture.dependencies.api.taskDetail).toHaveBeenCalledWith("task-1"),
    );
    act(() => {
      testFixture.callbacks.onTaskEvent(completedEvent(11, "task-1"));
      testFixture.callbacks.onTaskEvent({
        id: 12,
        schema_version: 1,
        task_id: "task-2",
        kind: "task.completed",
        created_at: NOW,
        payload: { task: task("task-2", "completed", 12) },
      });
    });
    act(() => pendingDetail.resolve(detail("task-1", 10)));

    await waitFor(() => expect(result.current.state.selectedDetail).not.toBeNull());
    expect(result.current.state.selectedDetail?.task.status).toBe("completed");
    expect(result.current.state.selectedDetail?.event_cursor).toBe(11);
    expect(result.current.state.tasksById["task-2"]?.status).toBe("completed");
  });

  it("detects a same-tick review conflict synchronously before committing its cursor", async () => {
    const pendingDetail = deferred<TaskDetail>();
    const first = reviewEvidence(1, "changes_requested");
    const testFixture = fixture(() => pendingDetail.promise);
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalledWith(10));

    act(() => result.current.selectTask("task-1"));
    await waitFor(() => expect(testFixture.taskDetail).toHaveBeenCalledTimes(1));

    let thrown: unknown;
    await act(async () => {
      pendingDetail.resolve(detailWithReviews("task-1", [first]));
      await Promise.resolve();
      try {
        testFixture.callbacks.onTaskEvent(
          reviewEvent(11, "task-1", {
            ...structuredClone(first),
            summary: "Conflicting review payload.",
          }),
        );
      } catch (error) {
        thrown = error;
      }
    });

    expect(thrown).toEqual(
      expect.objectContaining({ name: "ReviewProjectionProtocolError" }),
    );
    expect(result.current.state.appliedEventId).toBe(10);
    expect(result.current.state.snapshotRecovery).toEqual({
      conflictEventId: 11,
      reason: "review_payload_conflict",
    });
    expect(result.current.state.selectedDetail?.reviews).toEqual([first]);
  });

  it("resumes recovery from the bootstrap watermark plus only later buffered events", async () => {
    const first = reviewEvidence(1, "changes_requested");
    const testFixture = fixture(async (taskId) =>
      detailWithReviews(taskId, [first]),
    );
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalledWith(10));

    act(() => result.current.selectTask("task-1"));
    await waitFor(() =>
      expect(result.current.state.selectedDetail?.reviews).toEqual([first]),
    );

    act(() => {
      expect(() =>
        testFixture.callbacks.onTaskEvent(
          reviewEvent(11, "task-1", {
            ...structuredClone(first),
            summary: "Conflicting review payload.",
          }),
        ),
      ).toThrow(/review projection conflict/);
      expect(() =>
        testFixture.callbacks.onTaskEvent(activityEvent(12, "task-1")),
      ).toThrow(/review projection conflict/);
    });

    let resumeCursor: number | undefined;
    act(() => {
      resumeCursor = testFixture.callbacks.onBootstrap({
        ...bootstrap(),
        latest_event_id: 11,
        tasks: [task("task-1", "running", 11), task("task-2")],
      });
    });

    expect(resumeCursor).toBe(12);
    expect(result.current.state.appliedEventId).toBe(12);
    expect(result.current.state.snapshotRecovery).toBeNull();
    expect(result.current.state.recoveryBuffer).toEqual([]);
    expect(
      result.current.state.liveBufferByTaskId["task-1"]?.map(({ id }) => id),
    ).toEqual([12]);
    expect(testFixture.start).toHaveBeenCalledTimes(1);
    expect(testFixture.start.mock.calls).toEqual([[10]]);
  });

  it("keeps an incomplete detail stale after refetch failure and retries on reselection", async () => {
    const first = reviewEvidence(1, "changes_requested");
    const second = reviewEvidence(2);
    const testFixture = fixture(
      vi
        .fn<(_: string) => Promise<TaskDetail>>()
        .mockResolvedValueOnce(detailWithReviews("task-1", []))
        .mockRejectedValueOnce(new Error("detail refetch failed"))
        .mockResolvedValueOnce(detailWithReviews("task-1", [first, second], 11)),
    );
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalledWith(10));

    act(() => result.current.selectTask("task-1"));
    await waitFor(() => expect(result.current.state.selectedDetail).not.toBeNull());

    act(() => {
      testFixture.callbacks.onTaskEvent(reviewEvent(11, "task-1", second));
    });

    expect(result.current.state.appliedEventId).toBe(11);
    expect(result.current.state.detailStale).toBe(true);
    await waitFor(() => {
      expect(testFixture.taskDetail).toHaveBeenCalledTimes(2);
      expect(result.current.state.detailError).toBe("detail refetch failed");
    });
    expect(result.current.state.detailStale).toBe(true);
    expect(result.current.state.detailLoading).toBe(false);

    act(() => result.current.selectTask("task-1"));
    await waitFor(() =>
      expect(result.current.state.selectedDetail?.reviews.map(({ round }) => round))
        .toEqual([1, 2]),
    );
    expect(testFixture.taskDetail).toHaveBeenCalledTimes(3);
    expect(result.current.state.detailStale).toBe(false);
    expect(result.current.state.appliedEventId).toBe(11);
  });

  it("actively bridges a buffered-review conflict into transport recovery before another cursor can commit", async () => {
    const first = reviewEvidence(1, "changes_requested");
    const authoritativeSecond = reviewEvidence(2, "changes_requested");
    const second = {
      ...structuredClone(authoritativeSecond),
      required_checks: [
        ...authoritativeSecond.required_checks,
        {
          id: "unexpected-check",
          kind: "cargo_check" as const,
          package: null,
        },
      ],
      added_required_checks: [],
    };
    const testFixture = fixture(
      vi
        .fn<(_: string) => Promise<TaskDetail>>()
        .mockResolvedValueOnce(detailWithReviews("task-1", []))
        .mockResolvedValueOnce(detailWithReviews("task-1", [first], 10))
        .mockResolvedValueOnce(
          detailWithReviews("task-1", [first, authoritativeSecond], 12),
        ),
    );
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalledWith(10));

    act(() => result.current.selectTask("task-1"));
    await waitFor(() => expect(result.current.state.selectedDetail).not.toBeNull());

    act(() => {
      testFixture.callbacks.onTaskEvent(reviewEvent(11, "task-1", second));
    });
    expect(result.current.state.appliedEventId).toBe(11);

    await waitFor(() => {
      expect(testFixture.taskDetail).toHaveBeenCalledTimes(2);
      expect(result.current.state.snapshotRecovery).toEqual({
        conflictEventId: 11,
        reason: "review_history_conflict",
      });
    });
    expect(testFixture.requestRecovery).toHaveBeenCalledWith(
      "review_history_conflict",
    );

    expect(() =>
      testFixture.callbacks.onTaskEvent(activityEvent(12, "task-1")),
    ).toThrow(/review projection conflict/);
    expect(result.current.state.appliedEventId).toBe(11);

    let resumeCursor: number | undefined;
    act(() => {
      resumeCursor = testFixture.callbacks.onBootstrap({
        ...bootstrap(),
        latest_event_id: 12,
        tasks: [task("task-1", "running", 12), task("task-2")],
      });
    });

    expect(resumeCursor).toBe(12);
    expect(result.current.state.snapshotRecovery).toBeNull();
    expect(result.current.state.appliedEventId).toBe(12);
    await waitFor(() => expect(testFixture.taskDetail).toHaveBeenCalledTimes(3));
  });

  it("does not let a slower old detail response replace the current selection", async () => {
    const first = deferred<TaskDetail>();
    const second = deferred<TaskDetail>();
    const testFixture = fixture((taskId) =>
      taskId === "task-1" ? first.promise : second.promise,
    );
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalled());

    act(() => result.current.selectTask("task-1"));
    act(() => result.current.selectTask("task-2"));
    await waitFor(() =>
      expect(testFixture.dependencies.api.taskDetail).toHaveBeenCalledWith("task-2"),
    );

    act(() => second.resolve(detail("task-2")));
    await waitFor(() =>
      expect(result.current.state.selectedDetail?.task.id).toBe("task-2"),
    );
    act(() => first.resolve(detail("task-1")));

    await act(async () => Promise.resolve());
    expect(result.current.state.selectedTaskId).toBe("task-2");
    expect(result.current.state.selectedDetail?.task.id).toBe("task-2");
  });

  it("re-fetches detail when the same task is selected again", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalled());

    act(() => result.current.selectTask("task-1"));
    await waitFor(() =>
      expect(testFixture.dependencies.api.taskDetail).toHaveBeenCalledTimes(1),
    );
    act(() => result.current.selectTask("task-1"));
    await waitFor(() =>
      expect(testFixture.dependencies.api.taskDetail).toHaveBeenCalledTimes(2),
    );
  });

  it("rolls back optimistic cancel state when the mutation fails", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    testFixture.cancelTask.mockRejectedValueOnce({
      code: "STORE_BUSY",
      message: "busy",
      retryable: true,
      requestId: "cancel-request-id",
    });
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalled());

    let rejection: unknown;
    await act(async () => {
      try {
        await result.current.cancelTask("task-1");
      } catch (error) {
        rejection = error;
      }
    });

    expect(rejection).toEqual(expect.objectContaining({ code: "STORE_BUSY" }));
    expect(result.current.state.commands.cancelByTaskId["task-1"]).toEqual(
      expect.objectContaining({
        phase: "error",
        optimistic: false,
        error: expect.objectContaining({ requestId: "cancel-request-id" }),
      }),
    );
  });

  it("enters session-expired when initial bootstrap returns 401", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    testFixture.initialize.mockRejectedValueOnce({
      status: 401,
      code: "SESSION_EXPIRED",
      message: "reopen",
      retryable: false,
    });
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));

    await waitFor(() => expect(result.current.state.connection).toBe("session_expired"));
    expect(testFixture.start).not.toHaveBeenCalled();
  });

  it("stops the live stream when the hook unmounts", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    const { unmount } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalled());

    unmount();

    expect(testFixture.stop).toHaveBeenCalledTimes(1);
  });

  it("expires the session and stops SSE when detail fetch returns 401", async () => {
    const testFixture = fixture(async () => {
      throw sessionExpiredError();
    });
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalled());

    act(() => result.current.selectTask("task-1"));

    await waitFor(() => expect(result.current.state.connection).toBe("session_expired"));
    expect(testFixture.stop).toHaveBeenCalledTimes(1);
    expect(result.current.state.detailError).toBeNull();
  });

  it("expires the session, stops SSE, and clears cancel optimism on cancel 401", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    testFixture.cancelTask.mockRejectedValueOnce(sessionExpiredError());
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalled());

    await act(async () => {
      await result.current.cancelTask("task-1").catch(() => undefined);
    });

    expect(result.current.state.connection).toBe("session_expired");
    expect(result.current.state.commands.cancelByTaskId).toEqual({});
    expect(testFixture.stop).toHaveBeenCalledTimes(1);
  });

  it("upserts command results and preserves one create UUID across execute retries", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    testFixture.createTaskExecute
      .mockReset()
      .mockRejectedValueOnce({
        status: 0,
        code: "NETWORK_ERROR",
        message: "ambiguous",
        retryable: true,
      })
      .mockResolvedValueOnce(task("task-created", "queued", 11));
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalled());

    await act(async () => {
      await result.current.addRepository("C:/chosen");
      await result.current.pickRepository();
      await result.current.retryTask("task-1");
    });
    const command = result.current.newCreateTask("repo-1", "ship it");
    await act(async () => {
      await command.execute().catch(() => undefined);
      await command.execute();
      await result.current.quit();
    });

    expect(command.clientRequestId).toBe("stable-create-id");
    expect(testFixture.newCreateTask).toHaveBeenCalledTimes(1);
    expect(testFixture.createTaskExecute).toHaveBeenCalledTimes(2);
    expect(result.current.state.repositoryOrder).toEqual([
      SCHEDULER_REPOSITORY_ID,
      "repo-added",
      "repo-picked",
    ]);
    expect(result.current.state.taskOrder).toEqual([
      "task-1",
      "task-2",
      "task-retry",
      "task-created",
    ]);
    expect(testFixture.quit).toHaveBeenCalledTimes(1);
  });

  it("preserves queue-full create replay input and clears it after the same command succeeds", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    testFixture.createTaskExecute
      .mockReset()
      .mockRejectedValueOnce({
        status: 429,
        code: "TASK_QUEUE_FULL",
        message: "the task queue is full",
        retryable: true,
        requestId: "queue-full-http-request",
      })
      .mockResolvedValueOnce(task("task-created", "queued", 11));
    const { result } = renderHook(() => useAgentState(testFixture.dependencies));
    await waitFor(() => expect(testFixture.start).toHaveBeenCalled());
    const command = result.current.newCreateTask("repo-1", "original prompt");

    await act(async () => {
      await command.execute().catch(() => undefined);
    });

    expect(result.current.state.commands.queueFullReplay).toEqual({
      repositoryId: "repo-1",
      prompt: "original prompt",
      clientRequestId: "stable-create-id",
      requestId: "queue-full-http-request",
    });
    expect(command).toMatchObject({
      clientRequestId: "stable-create-id",
    });

    await act(async () => {
      await command.execute();
    });
    expect(result.current.state.commands.queueFullReplay).toBeNull();
  });

  it.each(["add", "pick", "create", "retry", "quit"] as const)(
    "expires the session and stops SSE when the %s command returns 401",
    async (commandName) => {
      const testFixture = fixture(async (taskId) => detail(taskId));
      const expired = sessionExpiredError();
      switch (commandName) {
        case "add":
          testFixture.addRepository.mockRejectedValueOnce(expired);
          break;
        case "pick":
          testFixture.pickRepository.mockRejectedValueOnce(expired);
          break;
        case "create":
          testFixture.createTaskExecute.mockRejectedValueOnce(expired);
          break;
        case "retry":
          testFixture.retryTask.mockRejectedValueOnce(expired);
          break;
        case "quit":
          testFixture.quit.mockRejectedValueOnce(expired);
          break;
      }
      const { result } = renderHook(() => useAgentState(testFixture.dependencies));
      await waitFor(() => expect(testFixture.start).toHaveBeenCalled());

      await act(async () => {
        const invocation = (() => {
          switch (commandName) {
            case "add":
              return result.current.addRepository("C:/repo");
            case "pick":
              return result.current.pickRepository();
            case "create":
              return result.current.newCreateTask("repo-1", "work").execute();
            case "retry":
              return result.current.retryTask("task-1");
            case "quit":
              return result.current.quit();
          }
        })();
        await invocation.catch(() => undefined);
      });

      expect(result.current.state.connection).toBe("session_expired");
      expect(testFixture.stop).toHaveBeenCalledTimes(1);
    },
  );

  it("keeps a projection recovery cursor uncommitted across bootstrap failure and retries from the recovered watermark", async () => {
    const first = reviewEvidence(1, "changes_requested");
    const conflict = reviewEvent(11, STREAM_REVIEW_TASK_ID, {
      ...structuredClone(first),
      summary: "Conflicting review payload.",
    });
    const firstResponse = deferred<Response>();
    const recoveryDelay = deferred<void>();
    const reconnectDelay = deferred<void>();
    const pendingFetch = deferred<Response>();
    const fetchMock = vi
      .fn<typeof globalThis.fetch>()
      .mockImplementationOnce(async () => firstResponse.promise)
      .mockImplementationOnce((_input, init) => {
        init?.signal?.addEventListener(
          "abort",
          () =>
            pendingFetch.reject(
              new DOMException("stream request aborted", "AbortError"),
            ),
          { once: true },
        );
        return pendingFetch.promise;
      });
    const sleep = vi
      .fn<(_: number, __: AbortSignal) => Promise<void>>()
      .mockImplementationOnce(async () => recoveryDelay.promise)
      .mockImplementationOnce(async () => reconnectDelay.promise);
    const testFixture = fixture(async (taskId) =>
      detailWithReviews(taskId, [first]),
    );
    testFixture.initialize.mockResolvedValueOnce({
      ...bootstrap(),
      tasks: [task(STREAM_REVIEW_TASK_ID)],
    });
    testFixture.bootstrapRequest
      .mockReset()
      .mockRejectedValueOnce(new Error("bootstrap temporarily unavailable"))
      .mockResolvedValueOnce({
        ...bootstrap(),
        latest_event_id: 11,
        tasks: [task(STREAM_REVIEW_TASK_ID, "running", 11)],
      });
    testFixture.dependencies.createStream = createSseAgentStreamFactory(
      testFixture.dependencies.api,
      {
        fetch: fetchMock,
        sleep,
        baseDelayMs: 1,
        maxDelayMs: 1,
        jitter: () => 0,
      },
    );
    const { result, unmount } = renderHook(() =>
      useAgentState(testFixture.dependencies),
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));

    act(() => result.current.selectTask(STREAM_REVIEW_TASK_ID));
    await waitFor(() =>
      expect(result.current.state.selectedDetail?.reviews).toEqual([first]),
    );

    act(() => {
      firstResponse.resolve(
        new Response(
          `id: 11\nevent: review.updated\ndata: ${JSON.stringify(conflict)}\n\n`,
          {
            status: 200,
            headers: { "Content-Type": "text/event-stream; charset=utf-8" },
          },
        ),
      );
    });

    await waitFor(() => {
      expect(testFixture.bootstrapRequest).toHaveBeenCalledTimes(1);
      expect(result.current.state.connection).toBe("unavailable");
    });
    expect(result.current.state.appliedEventId).toBe(10);
    expect(result.current.state.snapshotRecovery?.conflictEventId).toBe(11);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    act(() => recoveryDelay.resolve());
    await waitFor(() => {
      expect(testFixture.bootstrapRequest).toHaveBeenCalledTimes(2);
      expect(result.current.state.appliedEventId).toBe(11);
      expect(result.current.state.snapshotRecovery).toBeNull();
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    act(() => reconnectDelay.resolve());
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(String(fetchMock.mock.calls[1]?.[0])).toContain("after=11");

    unmount();
    await act(async () => Promise.resolve());
  });

  it("retries bootstrap when an active transport recovery is requested by buffered detail replay", async () => {
    const first = reviewEvidence(1, "changes_requested");
    const authoritativeSecond = reviewEvidence(2, "changes_requested");
    const conflictingSecond = {
      ...structuredClone(authoritativeSecond),
      required_checks: [
        ...authoritativeSecond.required_checks,
        {
          id: "unexpected-check",
          kind: "cargo_check" as const,
          package: null,
        },
      ],
      added_required_checks: [],
    };
    const streamTask = (lastEventId: number): Task => ({
      ...task(STREAM_REVIEW_TASK_ID, "running", lastEventId),
      repository_id: "22222222-2222-4222-8222-222222222222",
      client_request_id: "33333333-3333-4333-8333-333333333333",
      started_at: NOW,
    });
    const streamDetail = (
      reviews: ReviewEvidence[],
      cursor: number,
    ): TaskDetail => ({
      ...detailWithReviews(STREAM_REVIEW_TASK_ID, reviews, cursor),
      task: streamTask(cursor),
    });
    const recoveryDelay = deferred<void>();
    const reconnectDelay = deferred<void>();
    const firstResponse = deferred<Response>();
    const pendingFetch = deferred<Response>();
    let firstSignal: AbortSignal | null = null;
    const fetchMock = vi
      .fn<typeof globalThis.fetch>()
      .mockImplementationOnce((_input, init) => {
        firstSignal = init?.signal ?? null;
        return firstResponse.promise;
      })
      .mockImplementationOnce((_input, init) => {
        init?.signal?.addEventListener(
          "abort",
          () =>
            pendingFetch.reject(
              new DOMException("stream request aborted", "AbortError"),
            ),
          { once: true },
        );
        return pendingFetch.promise;
      });
    const sleep = vi
      .fn<(_: number, __: AbortSignal) => Promise<void>>()
      .mockImplementationOnce(async () => recoveryDelay.promise)
      .mockImplementationOnce(async () => reconnectDelay.promise);
    const details = vi
      .fn<(_: string) => Promise<TaskDetail>>()
      .mockResolvedValueOnce(streamDetail([], 10))
      .mockResolvedValueOnce(streamDetail([first], 10))
      .mockResolvedValueOnce(
        streamDetail([first, authoritativeSecond], 11),
      );
    const testFixture = fixture(details);
    testFixture.initialize.mockResolvedValueOnce({
      ...bootstrap(),
      tasks: [streamTask(10)],
    });
    testFixture.bootstrapRequest
      .mockReset()
      .mockRejectedValueOnce(new Error("bootstrap temporarily unavailable"))
      .mockResolvedValueOnce({
        ...bootstrap(),
        latest_event_id: 11,
        tasks: [streamTask(11)],
      });
    testFixture.dependencies.createStream = createSseAgentStreamFactory(
      testFixture.dependencies.api,
      {
        fetch: fetchMock,
        sleep,
        baseDelayMs: 1,
        maxDelayMs: 1,
        jitter: () => 0,
      },
    );
    const { result, unmount } = renderHook(() =>
      useAgentState(testFixture.dependencies),
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));

    act(() => result.current.selectTask(STREAM_REVIEW_TASK_ID));
    await waitFor(() => expect(result.current.state.selectedDetail).not.toBeNull());

    const conflict = reviewEvent(
      11,
      STREAM_REVIEW_TASK_ID,
      conflictingSecond,
    );
    let frameSent = false;
    act(() => {
      const signal = firstSignal;
      firstResponse.resolve(
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              signal?.addEventListener(
                "abort",
                () => {
                  try {
                    controller.error(
                      new DOMException("stream request aborted", "AbortError"),
                    );
                  } catch {
                    // The reader may already have released the stream.
                  }
                },
                { once: true },
              );
            },
            pull(controller) {
              if (frameSent) {
                return;
              }
              frameSent = true;
              controller.enqueue(
                new TextEncoder().encode(
                  `id: 11\nevent: review.updated\ndata: ${JSON.stringify(conflict)}\n\n`,
                ),
              );
            },
          }),
          {
            status: 200,
            headers: { "Content-Type": "text/event-stream; charset=utf-8" },
          },
        ),
      );
    });

    await waitFor(() => {
      expect(testFixture.taskDetail).toHaveBeenCalledTimes(2);
      expect(testFixture.bootstrapRequest).toHaveBeenCalledTimes(1);
      expect(result.current.state.connection).toBe("unavailable");
    });
    expect(result.current.state.appliedEventId).toBe(11);
    expect(result.current.state.snapshotRecovery).toEqual({
      conflictEventId: 11,
      reason: "review_history_conflict",
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    act(() => recoveryDelay.resolve());
    await waitFor(() => {
      expect(testFixture.bootstrapRequest).toHaveBeenCalledTimes(2);
      expect(result.current.state.snapshotRecovery).toBeNull();
      expect(result.current.state.appliedEventId).toBe(11);
    });

    act(() => reconnectDelay.resolve());
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(String(fetchMock.mock.calls[1]?.[0])).toContain("after=11");

    unmount();
    await act(async () => Promise.resolve());
  });

  it("passes recovery AbortSignal and suppresses bootstrap projection after abort", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    const recovery = deferred<BootstrapResponse>();
    const called = deferred<AbortSignal | undefined>();
    testFixture.bootstrapRequest.mockImplementationOnce(async (signal) => {
      called.resolve(signal);
      return recovery.promise;
    });
    const onBootstrap = vi.fn();
    const factory = createSseAgentStreamFactory(testFixture.dependencies.api, {
      fetch: async () =>
        new Response("invalid type", {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
    });
    const stream = factory({
      onTaskEvent: vi.fn(),
      onSchedulerSnapshot: vi.fn(),
      onUnknownEvent: vi.fn(),
      onServiceState: vi.fn(),
      onBootstrap,
      onConnectionState: vi.fn(),
      onSessionExpired: vi.fn(),
    });

    const running = stream.start(10);
    const signal = await called.promise;
    stream.stop();
    recovery.resolve(bootstrap());
    await running;

    expect(signal).toBeDefined();
    expect(signal?.aborted).toBe(true);
    expect(onBootstrap).not.toHaveBeenCalled();
  });

  it("forwards a complete scheduler snapshot through the default SSE bridge", async () => {
    const testFixture = fixture(async (taskId) => detail(taskId));
    const snapshot = {
      ...structuredClone(bootstrap().scheduler),
      generation: 2,
      active_task_count: 0,
      queued_task_count: 0,
      queued_tasks: [],
      stopping_tasks: [],
      storage: {
        state: "normal" as const,
        data: { state: "normal" as const },
        runtime: { state: "normal" as const },
        repositories: [],
      },
    };
    const digest = await schedulerStateDigest(snapshot);
    const control = {
      schema_version: 1,
      kind: "scheduler.state",
      server_instance_id: snapshot.server_instance_id,
      server_started_at: snapshot.server_started_at,
      generation: snapshot.generation,
      as_of_event_id: snapshot.as_of_event_id,
      service_state_generation: snapshot.service_state_generation,
      admission_state: snapshot.admission_state,
      limits: snapshot.limits,
      active_task_count: snapshot.active_task_count,
      queued_task_count: snapshot.queued_task_count,
      stopping_task_count: 0,
      repository_storage_count: 0,
      storage: {
        state: snapshot.storage.state,
        data: snapshot.storage.data,
        runtime: snapshot.storage.runtime,
      },
      item_count: 0,
      chunk_count: 0,
      snapshot_digest: digest,
    };
    const received = deferred<SchedulerSnapshotCandidate>();
    const factory = createSseAgentStreamFactory(testFixture.dependencies.api, {
      fetch: async () =>
        new Response(
          `event: scheduler.state\ndata: ${JSON.stringify(control)}\n\n`,
          {
            status: 200,
            headers: { "Content-Type": "text/event-stream; charset=utf-8" },
          },
        ),
      baseDelayMs: 1,
      maxDelayMs: 1,
      jitter: () => 0,
    });
    const stream = factory({
      onTaskEvent: vi.fn(),
      onSchedulerSnapshot: (candidate) => received.resolve(candidate),
      onUnknownEvent: vi.fn(),
      onServiceState: vi.fn(),
      onBootstrap: (nextBootstrap) => nextBootstrap.latest_event_id,
      onConnectionState: vi.fn(),
      onSessionExpired: vi.fn(),
    });

    const running = stream.start(10);
    const candidate = await received.promise;
    stream.stop();
    await running;

    expect(candidate).toMatchObject({ snapshot, digest });
  });
});
