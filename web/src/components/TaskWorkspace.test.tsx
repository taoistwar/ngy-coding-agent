import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { SchedulerStoppingTask, Task, TaskDetail } from "../api/types";
import type { CancelCommandState } from "../state/model";
import { initialDeliveryState } from "../state/deliveryModel";
import type { DeliveryPanelBinding } from "./DeliveryPanel";
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
    delivery_readiness: "unreviewed",
    attempt: 1,
    last_event_id: 10,
    created_at: NOW,
    retry_of: null,
    started_at: null,
    finished_at: null,
    failure: null,
    ...overrides,
  };
}

function detail(value: Task): TaskDetail {
  return {
    task: value,
    event_cursor: value.last_event_id,
    plan: {
      format_version: 1,
      revision: 1,
      summary: "Implement and verify the request",
      items: [
        {
          id: "understand",
          title: "Understand request",
          description: "Read the request and repository context",
          acceptance_criteria: ["The requested behavior is understood"],
          status: "completed",
        },
        {
          id: "implement",
          title: "Implement change",
          description: "Apply the scoped code changes",
          acceptance_criteria: ["The requested behavior is implemented"],
          status: "running",
        },
        {
          id: "verify",
          title: "Verify result",
          description: "Run the focused validation",
          acceptance_criteria: ["The focused tests pass"],
          status: "pending",
        },
      ],
      initial_required_checks: [],
    },
    activity: [
      {
        id: "a-1",
        level: "info",
        actor: "system",
        role_run: null,
        message: "Workspace prepared",
        created_at: NOW,
      },
      {
        id: "a-2",
        level: "warning",
        actor: "executor",
        role_run: 1,
        message: "Using fake runner",
        created_at: NOW,
      },
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
          truncated: false,
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
    reviews: [],
  };
}

function props(value: Task | null, overrides: Partial<TaskWorkspaceProps> = {}): TaskWorkspaceProps {
  return {
    task: value,
    detail: value === null ? null : detail(value),
    detailLoading: false,
    detailError: null,
    cancelState: undefined,
    schedulerQueuedTask: null,
    schedulerStoppingTask: null,
    tasksById: value === null ? {} : { [value.id]: value },
    taskOrder: value === null ? [] : [value.id],
    onCancel: vi.fn(),
    onRetry: vi.fn(),
    onSelectTask: vi.fn(),
    ...overrides,
  };
}

function deliveryBinding(taskId: string): DeliveryPanelBinding {
  return {
    api: {
      newPreflight: () => {
        throw new Error("unexpected preflight command");
      },
      newMerge: () => {
        throw new Error("unexpected merge command");
      },
      newRemoveWorktree: () => {
        throw new Error("unexpected worktree cleanup command");
      },
      newDeleteBranch: () => {
        throw new Error("unexpected branch cleanup command");
      },
    },
    controller: {
      state: {
        ...initialDeliveryState,
        taskId,
        generation: 1,
        phase: "loading",
      },
      refresh: vi.fn(),
      trackOperation: vi.fn(),
      openModal: vi.fn(),
      clearModal: vi.fn(),
    },
  };
}

describe("TaskWorkspace", () => {
  it("only assembles the independently injected delivery controller", () => {
    const running = task("task-delivery", "completed");
    render(
      <TaskWorkspace
        {...props(running, { delivery: deliveryBinding(running.id) })}
      />,
    );

    expect(screen.getByRole("region", { name: "Delivery" })).toBeVisible();
    expect(
      screen.getByRole("status", { name: "Delivery projection status" }),
    ).toHaveTextContent("Loading delivery eligibility");
    expect(screen.getByRole("heading", { name: "Plan" })).toBeVisible();
  });

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

  it.each([
    ["user_cancelled", "Stopping — user requested"],
    ["disk_pressure_critical", "Stopping — critical storage pressure"],
  ] as const)(
    "disables repeated cancel for the durable %s stop winner",
    (intent, label) => {
      const running = task(`task-${intent}`, "running", { started_at: NOW });
      const stopping: SchedulerStoppingTask = {
        task_id: running.id,
        intent,
      };

      render(
        <TaskWorkspace
          {...props(running, { schedulerStoppingTask: stopping })}
        />,
      );

      expect(screen.getByRole("status", { name: "Durable stop status" })).toHaveTextContent(
        label,
      );
      expect(screen.getByRole("button", { name: "Cancel task" })).toBeDisabled();
    },
  );

  it("does not infer a durable stop winner from local cancel pending state", () => {
    const running = task("task-local-cancel", "running", { started_at: NOW });
    const pending: CancelCommandState = {
      phase: "pending",
      optimistic: true,
      error: null,
    };

    render(<TaskWorkspace {...props(running, { cancelState: pending })} />);

    expect(screen.getByRole("button", { name: "Cancelling" })).toBeDisabled();
    expect(screen.queryByRole("status", { name: "Durable stop status" })).not.toBeInTheDocument();
  });

  it("ignores a stale stopping entry after terminal events and keeps final outcomes separate", () => {
    const cancelled = task("task-cancelled", "cancelled", {
      started_at: NOW,
      finished_at: NOW,
    });
    const staleStopping: SchedulerStoppingTask = {
      task_id: cancelled.id,
      intent: "user_cancelled",
    };
    const { rerender } = render(
      <TaskWorkspace
        {...props(cancelled, { schedulerStoppingTask: staleStopping })}
      />,
    );

    expect(screen.getByText("Final outcome: Cancelled")).toBeVisible();
    expect(screen.queryByText(/Stopping —/u)).not.toBeInTheDocument();

    const failed = task("task-disk-failed", "failed", {
      started_at: NOW,
      finished_at: NOW,
      failure: {
        code: "DISK_PRESSURE_CRITICAL",
        message: "Execution stopped to protect storage",
        retryable: true,
      },
    });
    rerender(
      <TaskWorkspace
        {...props(failed, {
          schedulerStoppingTask: {
            task_id: failed.id,
            intent: "disk_pressure_critical",
          },
        })}
      />,
    );

    expect(screen.getByText("Final outcome: Failed — retryable")).toBeVisible();
    expect(screen.queryByText(/Stopping —/u)).not.toBeInTheDocument();
    expect(screen.queryByText(/Cancelled|Review rejected/u)).not.toBeInTheDocument();
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
    expect(
      screen.getByText("Workspace prepared", { selector: ".activity-message" }),
    ).toBeVisible();
    expect(screen.getByRole("heading", { name: "Worktree diff" })).toBeVisible();
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

  it("keeps lifecycle and delivery readiness separate and never derives approval from reviews", () => {
    const completed = task("task-done", "completed", {
      delivery_readiness: "unreviewed",
      started_at: NOW,
      finished_at: NOW,
    });
    const value = detail(completed);
    value.reviews = [
      {
        round: 1,
        decision_source: "reviewer",
        workspace_generation: 3,
        workspace_digest: {
          algorithm: "workspace_fingerprint_v1",
          value: "a".repeat(64),
        },
        verdict: "approved",
        summary: "The bounded evidence is approved.",
        findings: [],
        added_required_checks: [],
        required_checks: [
          {
            id: "workspace-tests",
            kind: "cargo_test",
            package: null,
            integration_test: null,
          },
        ],
        check_evidence: [],
        coverage: null,
        created_at: NOW,
      },
    ];

    render(<TaskWorkspace {...props(completed, { detail: value })} />);

    expect(screen.getByText("Execution status: completed")).toBeVisible();
    expect(screen.getByText("Delivery readiness: unreviewed")).toBeVisible();
    expect(screen.getByText("Execution completed — not reviewed")).toBeVisible();
    expect(
      screen.queryByText("Delivery readiness: review approved"),
    ).not.toBeInTheDocument();
  });

  it("shows structured and legacy plans plus explicit role activity labels", () => {
    const running = task("task-plan", "running");
    const value = detail(running);
    value.plan!.initial_required_checks = [
      {
        id: "workspace-tests",
        kind: "cargo_test",
        package: "coding-agent-app",
        integration_test: "server",
      },
    ];
    value.activity.push(
      {
        id: "a-3",
        level: "info",
        actor: "planner",
        role_run: 1,
        message: "Plan submitted",
        created_at: NOW,
      },
      {
        id: "a-4",
        level: "info",
        actor: "reviewer",
        role_run: 2,
        message: "Review started",
        created_at: NOW,
      },
    );

    const { rerender } = render(
      <TaskWorkspace {...props(running, { detail: value })} />,
    );

    expect(screen.getByText("Implement and verify the request")).toBeVisible();
    expect(screen.getByText("Apply the scoped code changes")).toBeVisible();
    expect(screen.getByText("The requested behavior is implemented")).toBeVisible();
    expect(screen.getByText("Initial required checks")).toBeVisible();
    expect(
      screen.getByText(/cargo test.*coding-agent-app.*server/i),
    ).toBeVisible();
    expect(screen.getByText("System")).toBeVisible();
    expect(screen.getByText("Executor #1")).toBeVisible();
    expect(screen.getByText("Planner #1")).toBeVisible();
    expect(screen.getByText("Reviewer #2")).toBeVisible();

    value.plan = {
      format_version: 0,
      revision: 7,
      summary: "",
      items: [
        {
          id: "legacy",
          title: "Legacy step",
          description: "",
          acceptance_criteria: [],
          status: "completed",
        },
      ],
      initial_required_checks: [],
    };
    rerender(<TaskWorkspace {...props(running, { detail: value })} />);
    expect(
      screen.getByText(
        "Legacy plan: structured summary and acceptance criteria were not recorded.",
      ),
    ).toBeVisible();
  });

  it("orders evidence panels and labels diff and tests with workspace generation", () => {
    const failed = task("task-panel-order", "failed", {
      failure: { code: "FAILED", message: "Stopped", retryable: true },
    });
    const value = detail(failed);
    value.reviews = [
      {
        round: 1,
        decision_source: "reviewer",
        workspace_generation: 2,
        workspace_digest: {
          algorithm: "workspace_fingerprint_v1",
          value: "a".repeat(64),
        },
        verdict: "changes_requested",
        summary: "Changes are required.",
        findings: [],
        added_required_checks: [],
        required_checks: [
          {
            id: "workspace-tests",
            kind: "cargo_test",
            package: null,
            integration_test: null,
          },
        ],
        check_evidence: [],
        coverage: null,
        created_at: NOW,
      },
    ];

    render(<TaskWorkspace {...props(failed, { detail: value })} />);

    const aside = screen.getByRole("complementary", {
      name: "Results and evidence",
    });
    const headings = within(aside)
      .getAllByRole("heading", { level: 3 })
      .map((heading) => heading.textContent);
    expect(headings).toEqual([
      "Attempts",
      "Failure",
      "Review",
      "Worktree diff",
      "Test results",
      "Lifecycle timeline",
    ]);
    expect(within(aside).getByText("Workspace generation 2")).toBeVisible();
    expect(within(aside).getByText("Workspace generation 3")).toBeVisible();
    expect(
      within(aside).queryByRole("button", { name: /merge|approve|override/i }),
    ).not.toBeInTheDocument();
  });

  it("labels completed execution without implying review, delivery, merge, or editing", () => {
    const completed = task("task-done", "completed", {
      started_at: NOW,
      finished_at: NOW,
    });
    const { container } = render(<TaskWorkspace {...props(completed)} />);

    expect(screen.getByText("Execution completed — not reviewed")).toBeVisible();
    expect(container).not.toHaveTextContent(/review passed|deliverable|merge|edit code/i);
    expect(
      screen.getByText("Execution status: completed", {
        selector: ".task-status-label",
      }),
    ).toBeVisible();
    expect(screen.getByText(/Execution status: completed/i)).toBeVisible();
  });

  it("marks a bounded worktree patch when its true prefix was truncated", () => {
    const running = task("task-truncated-diff", "running");
    const bounded = detail(running);
    bounded.diff!.files[0]!.truncated = true;

    render(<TaskWorkspace {...props(running, { detail: bounded })} />);

    expect(screen.getByRole("heading", { name: "Worktree diff" })).toBeVisible();
    expect(screen.getByText("Patch truncated at safety limit")).toBeVisible();
    expect(screen.getByLabelText("Worktree patch for src/lib.rs")).toHaveTextContent(
      "@@ -1 +1 @@",
    );
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
      unaffectedHeading: "Worktree diff",
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
      unaffectedHeading: "Worktree diff",
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
      reviews: [],
    };
    render(<TaskWorkspace {...props(running, { detail: empty })} />);

    expect(screen.getByText("No plan has been published yet.")).toBeVisible();
    expect(screen.getByText("No activity yet.")).toBeVisible();
    expect(screen.getByText("No worktree diff is available yet.")).toBeVisible();
    expect(screen.getByText("No test results are available yet.")).toBeVisible();
    expect(screen.getByText("No lifecycle events are available yet.")).toBeVisible();
  });
});
