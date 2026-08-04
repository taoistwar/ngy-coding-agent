import {
  act,
  cleanup,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { CreateTaskCommand } from "../api/client";
import type { SchedulerState, Task } from "../api/types";
import type { QueueFullReplayState } from "../state/model";
import type { SchedulerProjectionState } from "../state/schedulerProjection";
import { TaskComposer } from "./TaskComposer";

const NOW = "2026-07-31T00:00:00Z";

afterEach(cleanup);

function task(): Task {
  return {
    id: "00000000-0000-4000-8000-000000000010",
    repository_id: "00000000-0000-4000-8000-000000000001",
    client_request_id: "client-created",
    prompt: "ship it",
    status: "queued",
    delivery_readiness: "unreviewed",
    attempt: 1,
    last_event_id: 1,
    created_at: NOW,
    retry_of: null,
    started_at: null,
    finished_at: null,
    failure: null,
  };
}

function scheduler(
  freshness: "fresh" | "stale",
  queuedTaskCount: number,
  queuedLimit: number,
  generation = 1,
): SchedulerProjectionState {
  const snapshot: SchedulerState = {
    schema_version: 1,
    server_instance_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    server_started_at: NOW,
    generation,
    as_of_event_id: 1,
    service_state_generation: 1,
    admission_state: "running",
    limits: {
      global: 2,
      per_repository: 2,
      queued: queuedLimit,
      cargo_jobs_per_task: 2,
    },
    active_task_count: 0,
    queued_task_count: queuedTaskCount,
    queued_tasks: Array.from({ length: queuedTaskCount }, (_, index) => ({
      task_id: `00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
      reason: "global_capacity",
    })),
    stopping_tasks: [],
    storage: {
      state: "normal",
      data: { state: "normal" },
      runtime: { state: "normal" },
      repositories: [],
    },
  };
  return {
    snapshot,
    freshness,
    staleReason: freshness === "stale" ? "connection_reconnecting" : null,
    digest: "a".repeat(64),
    canonicalJson: JSON.stringify(snapshot),
    pending: null,
    recoveryReason: null,
  };
}

function command(
  execute: CreateTaskCommand["execute"],
  clientRequestId = "stable-client-request-id",
): CreateTaskCommand {
  return { clientRequestId, execute };
}

describe("TaskComposer scheduler admission", () => {
  it("disables a new submission only when a fresh snapshot says the queue is full", async () => {
    const user = userEvent.setup();
    const onCreateTask = vi.fn();
    render(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 2, 2)}
        queueFullReplay={null}
        onCreateTask={onCreateTask}
        onCreated={vi.fn()}
      />,
    );

    await user.type(
      screen.getByRole("textbox", { name: "Task description" }),
      "ship it",
    );

    expect(screen.getByRole("button", { name: "Create task" })).toBeDisabled();
    expect(
      screen.getByText(
        "Task queue capacity is full. New task submission is disabled until capacity becomes available.",
      ),
    ).toBeVisible();
    expect(onCreateTask).not.toHaveBeenCalled();
  });

  it("does not use a stale full snapshot as an admission decision", async () => {
    const user = userEvent.setup();
    const execute = vi.fn(async () => task());
    const onCreateTask = vi.fn(() => command(execute));
    render(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("stale", 2, 2)}
        queueFullReplay={null}
        onCreateTask={onCreateTask}
        onCreated={vi.fn()}
      />,
    );

    await user.type(
      screen.getByRole("textbox", { name: "Task description" }),
      "ship it",
    );
    const submit = screen.getByRole("button", { name: "Create task" });

    expect(submit).toBeEnabled();
    expect(
      screen.queryByText(/New task submission is disabled until capacity/),
    ).not.toBeInTheDocument();
    await user.click(submit);
    await waitFor(() => expect(execute).toHaveBeenCalledTimes(1));
  });

  it("allows an explicit same-id retry for an unknown result even when the fresh queue is full", async () => {
    const user = userEvent.setup();
    const created = task();
    const execute = vi
      .fn<CreateTaskCommand["execute"]>()
      .mockRejectedValueOnce({
        code: "NETWORK_ERROR",
        message: "The create result is unknown.",
        requestId: "network-request-id",
        retryable: true,
      })
      .mockResolvedValueOnce(created);
    const onCreateTask = vi.fn(() => command(execute, "ambiguous-client-id"));
    const onCreated = vi.fn();
    const view = render(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 0, 1)}
        queueFullReplay={null}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    await user.type(
      screen.getByRole("textbox", { name: "Task description" }),
      "ship it",
    );
    await user.click(screen.getByRole("button", { name: "Create task" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "The create result is unknown.",
    );

    view.rerender(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 1, 1, 2)}
        queueFullReplay={null}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    const retry = screen.getByRole("button", { name: "Retry create task" });
    expect(retry).toBeEnabled();
    await user.click(retry);

    await waitFor(() => expect(onCreated).toHaveBeenCalledWith(created));
    expect(onCreateTask).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("allows an APP_SHUTTING_DOWN outcome-unknown retry when the fresh queue is full", async () => {
    const user = userEvent.setup();
    const created = task();
    const execute = vi
      .fn<CreateTaskCommand["execute"]>()
      .mockRejectedValueOnce({
        code: "APP_SHUTTING_DOWN",
        message: "the application is shutting down",
        requestId: "outcome-unknown-request-id",
        retryable: true,
      })
      .mockResolvedValueOnce(created);
    const onCreateTask = vi.fn(() => command(execute, "outcome-unknown-client-id"));
    const onCreated = vi.fn();
    const view = render(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 0, 1)}
        queueFullReplay={null}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    await user.type(
      screen.getByRole("textbox", { name: "Task description" }),
      "ship it",
    );
    await user.click(screen.getByRole("button", { name: "Create task" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "the application is shutting down",
    );

    view.rerender(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 1, 1, 2)}
        queueFullReplay={null}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    const retry = screen.getByRole("button", { name: "Retry create task" });
    expect(retry).toBeEnabled();
    await user.click(retry);
    await waitFor(() => expect(onCreated).toHaveBeenCalledWith(created));
    expect(onCreateTask).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("retains a queue-full request and replays the same command only after fresh capacity returns", async () => {
    const user = userEvent.setup();
    const created = task();
    const execute = vi
      .fn<CreateTaskCommand["execute"]>()
      .mockRejectedValueOnce({
        status: 429,
        code: "TASK_QUEUE_FULL",
        message: "the task queue is full; retry after capacity becomes available",
        requestId: "queue-full-request-id",
        retryable: true,
      })
      .mockResolvedValueOnce(created);
    const onCreateTask = vi.fn(() => command(execute, "queue-full-client-id"));
    const onCreated = vi.fn();
    const view = render(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 0, 1)}
        queueFullReplay={null}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    const input = screen.getByRole("textbox", { name: "Task description" });
    await user.type(input, "ship it");
    await user.click(screen.getByRole("button", { name: "Create task" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "the task queue is full",
    );

    const replay: QueueFullReplayState = {
      repositoryId: "00000000-0000-4000-8000-000000000001",
      prompt: "ship it",
      clientRequestId: "queue-full-client-id",
      requestId: "queue-full-request-id",
    };
    view.rerender(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 1, 1, 2)}
        queueFullReplay={replay}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    expect(input).toHaveValue("ship it");
    expect(screen.getByText("Request ID: queue-full-request-id")).toBeVisible();
    expect(
      screen.getByText("Client request ID: queue-full-client-id"),
    ).toBeVisible();
    const retry = screen.getByRole("button", { name: "Retry create task" });
    expect(retry).toBeDisabled();
    expect(
      screen.getByText(
        "This exact request can be retried when a fresh scheduler snapshot shows available queue capacity.",
      ),
    ).toBeVisible();
    await user.click(retry);
    expect(execute).toHaveBeenCalledTimes(1);

    view.rerender(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 0, 1, 3)}
        queueFullReplay={replay}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    expect(retry).toBeEnabled();
    await user.click(retry);
    await waitFor(() => expect(onCreated).toHaveBeenCalledWith(created));
    expect(onCreateTask).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("retains the exact queue-full command across a repository switch", async () => {
    const user = userEvent.setup();
    const created = task();
    const execute = vi
      .fn<CreateTaskCommand["execute"]>()
      .mockRejectedValueOnce({
        status: 429,
        code: "TASK_QUEUE_FULL",
        message: "the task queue is full; retry after capacity becomes available",
        requestId: "queue-full-request-id",
        retryable: true,
      })
      .mockResolvedValueOnce(created);
    const onCreateTask = vi.fn(() => command(execute, "queue-full-client-id"));
    const onCreated = vi.fn();
    const replay: QueueFullReplayState = {
      repositoryId: "00000000-0000-4000-8000-000000000001",
      prompt: "ship it",
      clientRequestId: "queue-full-client-id",
      requestId: "queue-full-request-id",
    };
    const view = render(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 0, 1)}
        queueFullReplay={null}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    const input = screen.getByRole("textbox", { name: "Task description" });
    await user.type(input, "ship it");
    await user.click(screen.getByRole("button", { name: "Create task" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "the task queue is full",
    );

    view.rerender(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000002"
        scheduler={scheduler("fresh", 0, 1, 2)}
        queueFullReplay={replay}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );
    await user.clear(input);
    await user.type(input, "work in the other repository");

    view.rerender(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 0, 1, 3)}
        queueFullReplay={replay}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    await waitFor(() => expect(input).toHaveValue("ship it"));
    const retry = screen.getByRole("button", { name: "Retry create task" });
    expect(retry).toBeEnabled();
    await user.click(retry);

    await waitFor(() => expect(onCreated).toHaveBeenCalledWith(created));
    expect(onCreateTask).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("retains a queue-full command whose response arrives after switching repositories", async () => {
    const user = userEvent.setup();
    const created = task();
    let rejectFirst: (reason: unknown) => void = () => {
      throw new Error("queue-full rejection was not installed");
    };
    const first = new Promise<Task>((_resolve, reject) => {
      rejectFirst = reject;
    });
    const execute = vi
      .fn<CreateTaskCommand["execute"]>()
      .mockImplementationOnce(() => first)
      .mockResolvedValueOnce(created);
    const onCreateTask = vi.fn(() => command(execute, "queue-full-client-id"));
    const onCreated = vi.fn();
    const replay: QueueFullReplayState = {
      repositoryId: "00000000-0000-4000-8000-000000000001",
      prompt: "ship it",
      clientRequestId: "queue-full-client-id",
      requestId: "queue-full-request-id",
    };
    const view = render(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 0, 1)}
        queueFullReplay={null}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    const input = screen.getByRole("textbox", { name: "Task description" });
    await user.type(input, "ship it");
    await user.click(screen.getByRole("button", { name: "Create task" }));
    await waitFor(() => expect(execute).toHaveBeenCalledTimes(1));

    view.rerender(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000002"
        scheduler={scheduler("fresh", 0, 1, 2)}
        queueFullReplay={null}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );
    await act(async () => {
      rejectFirst({
        status: 429,
        code: "TASK_QUEUE_FULL",
        message: "the task queue is full; retry after capacity becomes available",
        requestId: "queue-full-request-id",
        retryable: true,
      });
      await first.catch(() => undefined);
    });

    view.rerender(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 0, 1, 3)}
        queueFullReplay={replay}
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    await waitFor(() => expect(input).toHaveValue("ship it"));
    const retry = screen.getByRole("button", { name: "Retry create task" });
    expect(retry).toBeEnabled();
    await user.click(retry);
    await waitFor(() => expect(onCreated).toHaveBeenCalledWith(created));
    expect(onCreateTask).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("blocks an ordinary retryable response when a fresh snapshot is full", async () => {
    const user = userEvent.setup();
    const execute = vi.fn<CreateTaskCommand["execute"]>().mockRejectedValue({
      status: 503,
      code: "STORE_BUSY",
      message: "the local store is busy; retry the request",
      requestId: "store-busy-request-id",
      retryable: true,
    });
    const onCreateTask = vi.fn(() => command(execute, "store-busy-client-id"));
    const view = render(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 0, 1)}
        queueFullReplay={null}
        onCreateTask={onCreateTask}
        onCreated={vi.fn()}
      />,
    );

    await user.type(
      screen.getByRole("textbox", { name: "Task description" }),
      "ship it",
    );
    await user.click(screen.getByRole("button", { name: "Create task" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "the local store is busy",
    );

    view.rerender(
      <TaskComposer
        repositoryId="00000000-0000-4000-8000-000000000001"
        scheduler={scheduler("fresh", 1, 1, 2)}
        queueFullReplay={null}
        onCreateTask={onCreateTask}
        onCreated={vi.fn()}
      />,
    );

    expect(screen.getByRole("button", { name: "Retry create task" })).toBeDisabled();
    expect(
      screen.getByText(
        "Task queue capacity is full. New task submission is disabled until capacity becomes available.",
      ),
    ).toBeVisible();
    expect(onCreateTask).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(1);
  });
});
