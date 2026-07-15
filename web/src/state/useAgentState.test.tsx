import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type {
  BootstrapResponse,
  CancellationAcceptedResponse,
  Repository,
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

function repository(id = "repo-1"): Repository {
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
    repository_id: "repo-1",
    client_request_id: `request-${id}`,
    prompt: id,
    status,
    attempt: 1,
    last_event_id: cursor,
    created_at: NOW,
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
  const newCreateTask = vi.fn(() => ({
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
      return { start, stop };
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
      "repo-1",
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
});
