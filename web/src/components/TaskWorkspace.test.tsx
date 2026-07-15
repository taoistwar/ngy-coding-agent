import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { Task, TaskDetail } from "../api/types";
import type { CancelCommandState } from "../state/model";
import { TaskWorkspace, type TaskWorkspaceProps } from "./TaskWorkspace";

const NOW = "2026-07-15T00:00:00Z";

afterEach(cleanup);

function task(
  id: string,
  status: Task["status"],
  overrides: Partial<Task> = {},
): Task {
  return {
    id,
    repository_id: "repo-1",
    client_request_id: `request-${id}`,
    prompt: `Implement ${id}`,
    status,
    attempt: 1,
    last_event_id: 10,
    created_at: NOW,
    ...overrides,
  };
}

function detail(value: Task): TaskDetail {
  return {
    task: value,
    event_cursor: value.last_event_id,
    plan: {
      revision: 1,
      items: [
        { id: "understand", title: "Understand request", status: "completed" },
        { id: "implement", title: "Implement change", status: "running" },
        { id: "verify", title: "Verify result", status: "pending" },
      ],
    },
    activity: [
      { id: "a-1", level: "info", message: "Workspace prepared", created_at: NOW },
      { id: "a-2", level: "warning", message: "Using fake runner", created_at: NOW },
    ],
    diff: {
      revision: 2,
      files: [
        {
          path: "src/lib.rs",
          status: "modified",
          patch: "@@ -1 +1 @@\n-old\n+new",
          additions: 1,
          deletions: 1,
        },
      ],
    },
    tests: {
      revision: 3,
      status: "failed",
      cases: [
        {
          id: "case-1",
          name: "workspace renders",
          status: "failed",
          duration_ms: 12,
          summary: "expected panel",
        },
      ],
    },
    timeline: [
      {
        event_id: 10,
        kind: "task.failed",
        label: "Execution failed",
        created_at: NOW,
        failure: { code: "FAKE_FAILURE", message: "Synthetic failure", retryable: true },
      },
    ],
  };
}

function props(value: Task | null, overrides: Partial<TaskWorkspaceProps> = {}): TaskWorkspaceProps {
  return {
    task: value,
    detail: value === null ? null : detail(value),
    detailLoading: false,
    detailError: null,
    cancelState: undefined,
    tasksById: value === null ? {} : { [value.id]: value },
    taskOrder: value === null ? [] : [value.id],
    onCancel: vi.fn(),
    onRetry: vi.fn(),
    onSelectTask: vi.fn(),
    ...overrides,
  };
}

describe("TaskWorkspace", () => {
  it("renders complete empty, loading, and error states", () => {
    const { rerender } = render(<TaskWorkspace {...props(null)} />);
    expect(screen.getByRole("heading", { name: "No task selected" })).toBeVisible();
    expect(screen.getByText("Choose a task from the sidebar to inspect its execution.")).toBeVisible();

    const running = task("task-running", "running");
    rerender(
      <TaskWorkspace
        {...props(running, { detail: null, detailLoading: true })}
      />,
    );
    expect(screen.getByRole("status", { name: "Loading task details" })).toBeVisible();

    rerender(
      <TaskWorkspace
        {...props(running, {
          detail: null,
          detailLoading: false,
          detailError: "Detail service unavailable",
        })}
      />,
    );
    expect(screen.getByRole("alert")).toHaveTextContent("Detail service unavailable");
    expect(screen.getByRole("button", { name: "Cancel task" })).toBeEnabled();
  });

  it.each(["queued", "running"] as const)(
    "allows cancelling a %s task",
    async (status) => {
      const user = userEvent.setup();
      const value = task(`task-${status}`, status);
      const onCancel = vi.fn();
      render(<TaskWorkspace {...props(value, { onCancel })} />);

      await user.click(screen.getByRole("button", { name: "Cancel task" }));

      expect(onCancel).toHaveBeenCalledWith(value.id);
      expect(screen.getByText(new RegExp(`Status: ${status}`, "i"))).toBeVisible();
    },
  );

  it("shows a disabled local cancelling state and yields to terminal props", () => {
    const running = task("task-1", "running");
    const pending: CancelCommandState = {
      phase: "pending",
      optimistic: true,
      error: null,
    };
    const { rerender } = render(
      <TaskWorkspace {...props(running, { cancelState: pending })} />,
    );

    expect(screen.getByRole("button", { name: "Cancelling" })).toBeDisabled();
    expect(screen.getByText("Cancelling")).toBeVisible();

    const completed = task("task-1", "completed", {
      started_at: NOW,
      finished_at: NOW,
    });
    rerender(
      <TaskWorkspace
        {...props(completed, {
          detail: detail(completed),
          cancelState: undefined,
        })}
      />,
    );

    expect(screen.getByText("Execution completed — not reviewed")).toBeVisible();
    expect(screen.queryByText("Cancelling")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry task" })).toBeEnabled();
  });

  it("shows cancel and retry request failures beside their actions", async () => {
    const user = userEvent.setup();
    const running = task("task-cancel-error", "running");
    const cancelError: CancelCommandState = {
      phase: "error",
      optimistic: false,
      error: {
        code: "STORE_BUSY",
        message: "Storage is busy.",
        retryable: true,
        requestId: "cancel-request-id",
      },
    };
    const { rerender } = render(
      <TaskWorkspace {...props(running, { cancelState: cancelError })} />,
    );

    expect(screen.getByRole("alert")).toHaveTextContent("Storage is busy.");
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Request ID: cancel-request-id",
    );

    const failed = task("task-retry-error", "failed", {
      failure: { code: "FAILED", message: "Synthetic failure", retryable: true },
    });
    const onRetry = vi.fn().mockRejectedValue({
      code: "STORE_BUSY",
      message: "Retry could not be created.",
      requestId: "retry-request-id",
      retryable: true,
    });
    rerender(<TaskWorkspace {...props(failed, { onRetry })} />);
    await user.click(screen.getByRole("button", { name: "Retry task" }));

    const retryAlert = await screen.findByRole("alert");
    expect(retryAlert).toHaveTextContent("Retry could not be created.");
    expect(retryAlert).toHaveTextContent("Request ID: retry-request-id");
  });

  it.each(["completed", "failed", "cancelled", "interrupted"] as const)(
    "offers retry for the latest %s attempt and navigates to the returned attempt",
    async (status) => {
      const user = userEvent.setup();
      const oldTask = task(`task-${status}`, status, {
        started_at: NOW,
        finished_at: NOW,
        failure:
          status === "failed" || status === "interrupted"
            ? { code: "STOPPED", message: "Execution stopped", retryable: true }
            : null,
      });
      const nextTask = task(`task-${status}-retry`, "queued", {
        attempt: 2,
        retry_of: oldTask.id,
      });
      const onRetry = vi.fn().mockResolvedValue(nextTask);
      const onSelectTask = vi.fn();
      render(
        <TaskWorkspace
          {...props(oldTask, { onRetry, onSelectTask })}
        />,
      );

      await user.click(screen.getByRole("button", { name: "Retry task" }));

      expect(onRetry).toHaveBeenCalledWith(oldTask.id);
      expect(onSelectTask).toHaveBeenCalledWith(nextTask.id);
    },
  );

  it("renders a linear retry chain and keeps older attempts selectable but read-only", async () => {
    const user = userEvent.setup();
    const first = task("task-1", "failed", {
      attempt: 1,
      started_at: NOW,
      finished_at: NOW,
      failure: { code: "FAIL", message: "First failed", retryable: true },
    });
    const second = task("task-2", "cancelled", {
      attempt: 2,
      retry_of: first.id,
      started_at: NOW,
      finished_at: NOW,
    });
    const third = task("task-3", "running", {
      attempt: 3,
      retry_of: second.id,
      started_at: NOW,
    });
    const onSelectTask = vi.fn();
    render(
      <TaskWorkspace
        {...props(first, {
          detail: detail(first),
          tasksById: { [first.id]: first, [second.id]: second, [third.id]: third },
          taskOrder: [first.id, second.id, third.id],
          onSelectTask,
        })}
      />,
    );

    expect(screen.getByText("Read-only attempt")).toBeVisible();
    expect(screen.queryByRole("button", { name: "Retry task" })).not.toBeInTheDocument();
    const thirdAttempt = screen.getByRole("button", { name: /Attempt 3.*running/i });
    thirdAttempt.focus();
    await user.keyboard("{Enter}");
    expect(onSelectTask).toHaveBeenCalledWith(third.id);
  });

  it("keeps retry pending scoped to its source and ignores a late navigation result", async () => {
    const user = userEvent.setup();
    const first = task("task-first", "failed", {
      failure: { code: "FAILED", message: "First failed", retryable: true },
    });
    const second = task("task-second", "cancelled");
    const retried = task("task-first-retry", "queued", {
      attempt: 2,
      retry_of: first.id,
    });
    let resolveRetry!: (value: Task) => void;
    const onRetry = vi.fn(
      () =>
        new Promise<Task>((resolve) => {
          resolveRetry = resolve;
        }),
    );
    const onSelectTask = vi.fn();
    const { rerender } = render(
      <TaskWorkspace {...props(first, { onRetry, onSelectTask })} />,
    );

    await user.click(screen.getByRole("button", { name: "Retry task" }));
    expect(screen.getByRole("button", { name: "Retrying" })).toBeDisabled();

    rerender(<TaskWorkspace {...props(second, { onRetry, onSelectTask })} />);
    expect(screen.getByRole("button", { name: "Retry task" })).toBeEnabled();

    await act(async () => resolveRetry(retried));
    await waitFor(() => expect(onRetry).toHaveBeenCalledWith(first.id));
    expect(onSelectTask).not.toHaveBeenCalled();
  });

  it("shows plan, live activity, synthetic evidence, tests, timeline, and structured failure", () => {
    const failed = task("task-failed", "failed", {
      started_at: NOW,
      finished_at: NOW,
      failure: { code: "FAKE_FAILURE", message: "Synthetic failure", retryable: true },
    });
    render(<TaskWorkspace {...props(failed)} />);

    expect(screen.getByRole("heading", { name: "Plan" })).toBeVisible();
    expect(screen.getAllByRole("listitem", { name: /Plan step/ })).toHaveLength(3);
    expect(screen.getByRole("log", { name: "Task activity" })).toHaveAttribute(
      "aria-live",
      "polite",
    );
    expect(screen.getByText("Workspace prepared")).toBeVisible();
    expect(screen.getByRole("heading", { name: "Synthetic diff" })).toBeVisible();
    expect(screen.getByText("src/lib.rs")).toBeVisible();
    expect(screen.getByRole("heading", { name: "Test results" })).toBeVisible();
    expect(screen.getByText("workspace renders")).toBeVisible();
    expect(screen.getByRole("heading", { name: "Lifecycle timeline" })).toBeVisible();
    expect(screen.getAllByText("FAKE_FAILURE").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Synthetic failure").length).toBeGreaterThan(0);
    expect(
      within(screen.getByRole("complementary", { name: "Results and evidence" }))
        .getByRole("heading", { name: "Attempts" }),
    ).toBeVisible();
  });

  it("labels completed execution without implying review, delivery, merge, or editing", () => {
    const completed = task("task-done", "completed", {
      started_at: NOW,
      finished_at: NOW,
    });
    const { container } = render(<TaskWorkspace {...props(completed)} />);

    expect(screen.getByText("Execution completed — not reviewed")).toBeVisible();
    expect(container).not.toHaveTextContent(/review passed|deliverable|merge|edit code/i);
    expect(screen.getByText(/Status: completed/i)).toBeVisible();
  });

  it("isolates a broken plan projection so activity and task actions remain usable", () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const running = task("task-broken-plan", "running");
    const value = detail(running);
    const brokenPlan = { revision: 2 } as NonNullable<TaskDetail["plan"]>;
    Object.defineProperty(brokenPlan, "items", {
      get() {
        throw new Error("broken plan projection");
      },
    });
    value.plan = brokenPlan;

    render(<TaskWorkspace {...props(running, { detail: value })} />);

    expect(screen.getByText("Plan unavailable")).toBeVisible();
    expect(screen.getByText("Workspace prepared")).toBeVisible();
    expect(screen.getByRole("button", { name: "Cancel task" })).toBeEnabled();
    consoleError.mockRestore();
  });

  it("recovers a failed evidence boundary when a newer detail snapshot arrives", () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const running = task("task-recover-plan", "running");
    const broken = detail(running);
    const brokenPlan = { revision: 1 } as NonNullable<TaskDetail["plan"]>;
    Object.defineProperty(brokenPlan, "items", {
      get() {
        throw new Error("broken plan projection");
      },
    });
    broken.plan = brokenPlan;
    const { rerender } = render(
      <TaskWorkspace {...props(running, { detail: broken })} />,
    );
    expect(screen.getByText("Plan unavailable")).toBeVisible();

    const recoveredTask = task(running.id, "running", { last_event_id: 11 });
    const recovered = detail(recoveredTask);
    rerender(
      <TaskWorkspace
        {...props(recoveredTask, { detail: recovered })}
      />,
    );

    expect(screen.getByRole("heading", { name: "Plan" })).toBeVisible();
    expect(screen.queryByText("Plan unavailable")).not.toBeInTheDocument();
    consoleError.mockRestore();
  });

  it.each([
    {
      panel: "activity",
      fallback: "Activity unavailable",
      unaffectedHeading: "Plan",
      breakDetail(value: TaskDetail) {
        value.activity = new Proxy(value.activity, {
          get(target, property, receiver) {
            if (property === "map") throw new Error("broken activity projection");
            return Reflect.get(target, property, receiver);
          },
        });
      },
    },
    {
      panel: "diff",
      fallback: "Diff unavailable",
      unaffectedHeading: "Test results",
      breakDetail(value: TaskDetail) {
        if (value.diff === null || value.diff === undefined) throw new Error("fixture");
        Object.defineProperty(value.diff, "files", {
          get() {
            throw new Error("broken diff projection");
          },
        });
      },
    },
    {
      panel: "tests",
      fallback: "Test results unavailable",
      unaffectedHeading: "Synthetic diff",
      breakDetail(value: TaskDetail) {
        if (value.tests === null || value.tests === undefined) throw new Error("fixture");
        Object.defineProperty(value.tests, "cases", {
          get() {
            throw new Error("broken test projection");
          },
        });
      },
    },
    {
      panel: "timeline",
      fallback: "Timeline unavailable",
      unaffectedHeading: "Synthetic diff",
      breakDetail(value: TaskDetail) {
        value.timeline = new Proxy(value.timeline, {
          get(target, property, receiver) {
            if (property === "map") throw new Error("broken timeline projection");
            return Reflect.get(target, property, receiver);
          },
        });
      },
    },
  ])(
    "isolates a broken $panel projection from sibling evidence and actions",
    ({ fallback, unaffectedHeading, breakDetail }) => {
      const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
      const running = task("task-broken-evidence", "running");
      const value = detail(running);
      breakDetail(value);

      render(<TaskWorkspace {...props(running, { detail: value })} />);

      expect(screen.getByText(fallback)).toBeVisible();
      expect(screen.getByRole("heading", { name: unaffectedHeading })).toBeVisible();
      expect(screen.getByRole("button", { name: "Cancel task" })).toBeEnabled();
      consoleError.mockRestore();
    },
  );

  it("gives every evidence area an explicit empty state", () => {
    const running = task("task-empty", "running");
    const empty: TaskDetail = {
      task: running,
      event_cursor: 1,
      plan: null,
      activity: [],
      diff: null,
      tests: null,
      timeline: [],
    };
    render(<TaskWorkspace {...props(running, { detail: empty })} />);

    expect(screen.getByText("No plan has been published yet.")).toBeVisible();
    expect(screen.getByText("No activity yet.")).toBeVisible();
    expect(screen.getByText("No synthetic diff is available yet.")).toBeVisible();
    expect(screen.getByText("No test results are available yet.")).toBeVisible();
    expect(screen.getByText("No lifecycle events are available yet.")).toBeVisible();
  });
});
