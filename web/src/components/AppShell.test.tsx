import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { CreateTaskCommand } from "../api/client";
import type { Repository, Task, TaskDetail } from "../api/types";
import { initialAgentState, type AgentState } from "../state/model";
import type { UseAgentStateResult } from "../state/useAgentState";
import { AppShell } from "./AppShell";
import { ConnectionBanner } from "./ConnectionBanner";
import { ErrorBoundary } from "./ErrorBoundary";
import { repositoryPathForDisplay, Sidebar } from "./Sidebar";
import { TaskComposer } from "./TaskComposer";

const EARLIER = "2026-07-14T00:00:00Z";
const LATER = "2026-07-15T00:00:00Z";

afterEach(cleanup);

function repository(
  id: string,
  lastOpenedAt = LATER,
  displayName = id,
): Repository {
  return {
    id,
    display_name: displayName,
    selected_path: `C:/${id}`,
    git_root: `C:/${id}`,
    cargo_workspace_root: `C:/${id}`,
    created_at: EARLIER,
    last_opened_at: lastOpenedAt,
  };
}

function task(
  id: string,
  repositoryId: string,
  createdAt = LATER,
  status: Task["status"] = "running",
): Task {
  return {
    id,
    repository_id: repositoryId,
    client_request_id: `client-${id}`,
    prompt: `Prompt ${id}`,
    status,
    delivery_readiness: "unreviewed",
    attempt: 1,
    last_event_id: 1,
    created_at: createdAt,
    retry_of: null,
    started_at: null,
    finished_at: null,
    failure: null,
  };
}

function detail(value: Task): TaskDetail {
  return {
    task: value,
    event_cursor: value.last_event_id,
    plan: null,
    activity: [],
    diff: null,
    tests: null,
    timeline: [],
    reviews: [],
  };
}

function agentFixture(overrides: Partial<AgentState> = {}) {
  const selectedTask = task("task-new", "repo-new");
  const state: AgentState = {
    ...initialAgentState,
    repositoriesById: {
      "repo-old": repository("repo-old", EARLIER, "Older repository"),
      "repo-new": repository("repo-new", LATER, "Newest repository"),
    },
    repositoryOrder: ["repo-old", "repo-new"],
    tasksById: {
      "task-old": task("task-old", "repo-new", EARLIER, "completed"),
      "task-new": selectedTask,
    },
    taskOrder: ["task-old", "task-new"],
    selectedTaskId: selectedTask.id,
    selectedDetail: detail(selectedTask),
    serviceState: "ready",
    connection: "live",
    ...overrides,
  };
  const created = task("task-created", "repo-new", LATER, "queued");
  const execute = vi.fn(async () => created);
  const command: CreateTaskCommand = {
    clientRequestId: "stable-client-request-id",
    execute,
  };
  const result: UseAgentStateResult = {
    state,
    selectTask: vi.fn(),
    addRepository: vi.fn(async (path) => ({
      ...repository("repo-added"),
      selected_path: path,
    })),
    pickRepository: vi.fn(async () => repository("repo-picked")),
    newCreateTask: vi.fn(() => command),
    cancelTask: vi.fn(async (taskId) => ({
      ...(state.tasksById[taskId] ?? selectedTask),
      status: "cancelled" as const,
    })),
    retryTask: vi.fn(async () => task("task-retried", "repo-new", LATER, "queued")),
    quit: vi.fn(async () => ({ status: "shutting_down" as const })),
  };
  return { result, command, execute, created };
}

describe("Sidebar", () => {
  it("hides Windows verbatim prefixes while preserving device paths", () => {
    expect(
      repositoryPathForDisplay(String.raw`\\?\D:\workspace\rust\twist_drive`),
    ).toBe(String.raw`D:\workspace\rust\twist_drive`);
    expect(
      repositoryPathForDisplay(String.raw`\\?\UNC\server\share\repository`),
    ).toBe(String.raw`\\server\share\repository`);
    expect(
      repositoryPathForDisplay(String.raw`\\?\Volume{01234567}\repository`),
    ).toBe(String.raw`\\?\Volume{01234567}\repository`);
    expect(repositoryPathForDisplay("/workspace/repository")).toBe(
      "/workspace/repository",
    );
  });

  it("sorts repositories and tasks newest first and keeps every status textual", async () => {
    const user = userEvent.setup();
    const onSelectRepository = vi.fn();
    render(
      <Sidebar
        repositories={[
          repository("repo-old", EARLIER, "Older repository"),
          repository("repo-new", LATER, "Newest repository"),
        ]}
        tasks={[
          task("task-old", "repo-new", EARLIER, "completed"),
          task("task-new", "repo-new", LATER, "running"),
          task("other", "repo-old", LATER, "failed"),
        ]}
        selectedRepositoryId="repo-new"
        selectedTaskId="task-new"
        onSelectRepository={onSelectRepository}
        onSelectTask={vi.fn()}
        onAddRepository={vi.fn()}
        onPickRepository={vi.fn()}
        onRetry={vi.fn()}
      />,
    );

    const repositories = within(
      screen.getByRole("list", { name: "Repositories" }),
    ).getAllByRole("button");
    expect(repositories.map((button) => button.textContent)).toEqual([
      expect.stringContaining("Newest repository"),
      expect.stringContaining("Older repository"),
    ]);

    const taskItems = within(screen.getByRole("list", { name: "Tasks" })).getAllByRole(
      "listitem",
    );
    expect(taskItems[0]).toHaveTextContent("Prompt task-new");
    expect(taskItems[0]).toHaveTextContent("Running");
    expect(taskItems[1]).toHaveTextContent("Prompt task-old");
    expect(taskItems[1]).toHaveTextContent("Completed");
    expect(screen.queryByText("Prompt other")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /Older repository/ }));
    expect(onSelectRepository).toHaveBeenCalledWith("repo-old");
  });

  it("shows lifecycle and delivery readiness as separate task badges", () => {
    const approved: Task = {
      ...task("approved", "repo-new", LATER, "completed"),
      delivery_readiness: "review_approved",
    };
    const rejected: Task = {
      ...task("rejected", "repo-new", EARLIER, "failed"),
      delivery_readiness: "review_rejected",
    };

    render(
      <Sidebar
        repositories={[repository("repo-new")]}
        tasks={[approved, rejected]}
        selectedRepositoryId="repo-new"
        selectedTaskId={approved.id}
        onSelectRepository={vi.fn()}
        onSelectTask={vi.fn()}
        onAddRepository={vi.fn()}
        onPickRepository={vi.fn()}
        onRetry={vi.fn()}
      />,
    );

    const approvedItem = screen.getByText("Prompt approved").closest("li");
    const rejectedItem = screen.getByText("Prompt rejected").closest("li");
    expect(approvedItem).not.toBeNull();
    expect(rejectedItem).not.toBeNull();
    expect(within(approvedItem!).getByText("Completed")).toBeVisible();
    expect(within(approvedItem!).getByText("Review approved")).toBeVisible();
    expect(within(rejectedItem!).getByText("Failed")).toBeVisible();
    expect(within(rejectedItem!).getByText("Review rejected")).toBeVisible();
  });

  it("supports direct path registration, the native picker, retry, and empty states", async () => {
    const user = userEvent.setup();
    const onSelectRepository = vi.fn();
    const onSelectTask = vi.fn();
    const added = repository("repo-added");
    const picked = repository("repo-picked");
    const retried = task("task-retried", "repo-added", LATER, "queued");
    const onAddRepository = vi.fn(async () => added);
    const onPickRepository = vi.fn(async () => picked);
    const onRetry = vi.fn(async () => retried);

    const { rerender } = render(
      <Sidebar
        repositories={[]}
        tasks={[]}
        selectedRepositoryId={null}
        selectedTaskId={null}
        onSelectRepository={onSelectRepository}
        onSelectTask={onSelectTask}
        onAddRepository={onAddRepository}
        onPickRepository={onPickRepository}
        onRetry={onRetry}
      />,
    );
    expect(screen.getByText("No repositories yet.")).toBeVisible();
    expect(screen.getByText("Add or choose a repository to get started.")).toBeVisible();

    await user.type(screen.getByRole("textbox", { name: "Repository path" }), "  C:/work/repo  ");
    await user.click(screen.getByRole("button", { name: "Add repository path" }));
    await waitFor(() => expect(onAddRepository).toHaveBeenCalledWith("C:/work/repo"));
    expect(onSelectRepository).toHaveBeenCalledWith(added.id);

    await user.click(screen.getByRole("button", { name: "Choose repository folder" }));
    await waitFor(() => expect(onPickRepository).toHaveBeenCalledTimes(1));
    expect(onSelectRepository).toHaveBeenCalledWith(picked.id);

    const completed = task("terminal", "repo-added", LATER, "completed");
    rerender(
      <Sidebar
        repositories={[added]}
        tasks={[completed]}
        selectedRepositoryId={added.id}
        selectedTaskId={completed.id}
        onSelectRepository={onSelectRepository}
        onSelectTask={onSelectTask}
        onAddRepository={onAddRepository}
        onPickRepository={onPickRepository}
        onRetry={onRetry}
      />,
    );
    await user.click(screen.getByRole("button", { name: "Retry task Prompt terminal" }));
    await waitFor(() => expect(onRetry).toHaveBeenCalledWith(completed.id));
    expect(onSelectTask).toHaveBeenCalledWith(retried.id);
  });

  it("keeps an older attempt selectable without offering a branching retry", () => {
    const first = task("attempt-one", "repo", EARLIER, "failed");
    const second: Task = {
      ...task("attempt-two", "repo", LATER, "queued"),
      attempt: 2,
      retry_of: first.id,
    };

    render(
      <Sidebar
        repositories={[repository("repo")]}
        tasks={[first, second]}
        selectedRepositoryId="repo"
        selectedTaskId={first.id}
        onSelectRepository={vi.fn()}
        onSelectTask={vi.fn()}
        onAddRepository={vi.fn()}
        onPickRepository={vi.fn()}
        onRetry={vi.fn()}
      />,
    );

    expect(screen.getByText("Prompt attempt-one")).toBeVisible();
    expect(
      screen.queryByRole("button", { name: "Retry task Prompt attempt-one" }),
    ).not.toBeInTheDocument();
  });

  it("does not let a late sidebar retry override newer navigation", async () => {
    const user = userEvent.setup();
    const source = task("retry-source", "repo-new", LATER, "failed");
    const child: Task = {
      ...task("retry-child", "repo-new", LATER, "queued"),
      attempt: 2,
      retry_of: source.id,
    };
    let resolveRetry!: (value: Task) => void;
    const onRetry = vi.fn(
      () =>
        new Promise<Task>((resolve) => {
          resolveRetry = resolve;
        }),
    );
    const onSelectRepository = vi.fn();
    const onSelectTask = vi.fn();
    render(
      <Sidebar
        repositories={[
          repository("repo-new", LATER, "Current repository"),
          repository("repo-old", EARLIER, "Other repository"),
        ]}
        tasks={[source]}
        selectedRepositoryId="repo-new"
        selectedTaskId={source.id}
        onSelectRepository={onSelectRepository}
        onSelectTask={onSelectTask}
        onAddRepository={vi.fn()}
        onPickRepository={vi.fn()}
        onRetry={onRetry}
      />,
    );

    await user.click(screen.getByRole("button", { name: `Retry task ${source.prompt}` }));
    await user.click(screen.getByRole("button", { name: /Other repository/ }));
    expect(onSelectRepository).toHaveBeenCalledWith("repo-old");

    await act(async () => resolveRetry(child));
    expect(onSelectTask).not.toHaveBeenCalled();
  });
});

describe("TaskComposer", () => {
  it("states the isolated-worktree and trusted-code execution boundary", () => {
    render(
      <TaskComposer
        repositoryId="repo"
        onCreateTask={vi.fn()}
        onCreated={vi.fn()}
      />,
    );

    expect(screen.getByText(/isolated Git worktree/)).toBeVisible();
    expect(screen.getByText(/current user permissions/)).toBeVisible();
    expect(screen.getByText(/not a malicious-code sandbox/)).toBeVisible();
  });

  it("trims input, counts Unicode scalar values, and enforces the 50,000 limit", async () => {
    const user = userEvent.setup();
    const onCreateTask = vi.fn((): CreateTaskCommand => ({
      clientRequestId: "client-one",
      execute: vi.fn(async () => task("created", "repo")),
    }));
    render(
      <TaskComposer
        repositoryId="repo"
        onCreateTask={onCreateTask}
        onCreated={vi.fn()}
      />,
    );

    const input = screen.getByRole("textbox", { name: "Task description" });
    const submit = screen.getByRole("button", { name: "Create task" });
    await user.type(input, "   ");
    expect(submit).toBeDisabled();

    fireEvent.change(input, { target: { value: "😀" } });
    expect(screen.getByText("1 / 50,000 characters")).toBeVisible();

    fireEvent.change(input, { target: { value: "x".repeat(50_001) } });
    expect(screen.getByText("50,001 / 50,000 characters")).toBeVisible();
    expect(screen.getByText("Task descriptions must be 50,000 characters or fewer.")).toBeVisible();
    expect(submit).toBeDisabled();

    fireEvent.change(input, { target: { value: "  ship it 😀  " } });
    await user.click(submit);
    await waitFor(() => expect(onCreateTask).toHaveBeenCalledWith("repo", "ship it 😀"));
  });

  it("reuses one command for an explicit ambiguous retry and displays request IDs", async () => {
    const user = userEvent.setup();
    const created = task("created", "repo", LATER, "queued");
    const execute = vi
      .fn<() => Promise<Task>>()
      .mockRejectedValueOnce({
        code: "NETWORK_ERROR",
        message: "The result is unknown.",
        requestId: "server-request-id",
        retryable: true,
      })
      .mockResolvedValueOnce(created);
    const onCreateTask = vi.fn(
      (): CreateTaskCommand => ({ clientRequestId: "stable-client-id", execute }),
    );
    const onCreated = vi.fn();
    render(
      <TaskComposer
        repositoryId="repo"
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    await user.type(screen.getByRole("textbox", { name: "Task description" }), "build it");
    await user.click(screen.getByRole("button", { name: "Create task" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("The result is unknown.");
    expect(screen.getByText("Request ID: server-request-id")).toBeVisible();
    expect(screen.getByText("Client request ID: stable-client-id")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Retry create task" }));

    await waitFor(() => expect(onCreated).toHaveBeenCalledWith(created));
    expect(onCreateTask).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("does not reuse an ambiguous create command after the repository changes", async () => {
    const user = userEvent.setup();
    const firstExecute = vi.fn().mockRejectedValue({
      code: "NETWORK_ERROR",
      message: "The result is unknown.",
      requestId: "first-request-id",
      retryable: true,
    });
    const secondCreated = task("created-in-second", "repo-second", LATER, "queued");
    const secondExecute = vi.fn(async () => secondCreated);
    const onCreateTask = vi.fn((repositoryId: string): CreateTaskCommand =>
      repositoryId === "repo-first"
        ? { clientRequestId: "first-client-id", execute: firstExecute }
        : { clientRequestId: "second-client-id", execute: secondExecute },
    );
    const onCreated = vi.fn();
    const { rerender } = render(
      <TaskComposer
        repositoryId="repo-first"
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );

    await user.type(screen.getByRole("textbox", { name: "Task description" }), "build it");
    await user.click(screen.getByRole("button", { name: "Create task" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("result is unknown");

    rerender(
      <TaskComposer
        repositoryId="repo-second"
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Create task" })).toBeEnabled(),
    );
    await user.click(screen.getByRole("button", { name: "Create task" }));

    await waitFor(() => expect(onCreated).toHaveBeenCalledWith(secondCreated));
    expect(onCreateTask).toHaveBeenLastCalledWith("repo-second", "build it");
    expect(firstExecute).toHaveBeenCalledTimes(1);
    expect(secondExecute).toHaveBeenCalledTimes(1);
  });

  it("discards a pending create failure after the repository changes", async () => {
    const user = userEvent.setup();
    let rejectFirst!: (error: unknown) => void;
    const firstExecute = vi.fn(
      () =>
        new Promise<Task>((_resolve, reject) => {
          rejectFirst = reject;
        }),
    );
    const secondCreated = task("created-second", "repo-second", LATER, "queued");
    const secondExecute = vi.fn(async () => secondCreated);
    const onCreateTask = vi.fn((repositoryId: string): CreateTaskCommand =>
      repositoryId === "repo-first"
        ? { clientRequestId: "first-client-id", execute: firstExecute }
        : { clientRequestId: "second-client-id", execute: secondExecute },
    );
    const onCreated = vi.fn();
    const { rerender } = render(
      <TaskComposer
        repositoryId="repo-first"
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );
    await user.type(screen.getByRole("textbox", { name: "Task description" }), "build it");
    await user.click(screen.getByRole("button", { name: "Create task" }));

    rerender(
      <TaskComposer
        repositoryId="repo-second"
        onCreateTask={onCreateTask}
        onCreated={onCreated}
      />,
    );
    await act(async () =>
      rejectFirst({
        code: "NETWORK_ERROR",
        message: "Late failure from the first repository.",
        requestId: "late-request-id",
        retryable: true,
      }),
    );

    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Create task" })).toBeEnabled();
    await user.click(screen.getByRole("button", { name: "Create task" }));
    await waitFor(() => expect(onCreated).toHaveBeenCalledWith(secondCreated));
    expect(onCreateTask).toHaveBeenLastCalledWith("repo-second", "build it");
  });
});

describe("ConnectionBanner", () => {
  it.each([
    ["live", "ready", false, "Connected"],
    ["reconnecting", "ready", false, "Reconnecting"],
    ["live", "store_degraded", false, "Store degraded"],
    ["live", "quiescing", false, "Shutting down"],
    ["session_expired", "ready", false, "Session expired"],
    ["unavailable", "ready", false, "Server unavailable"],
    ["live", "ready", true, "Shutting down"],
  ] as const)("maps %s/%s to %s", (connection, serviceState, quitting, label) => {
    render(
      <ConnectionBanner
        connection={connection}
        serviceState={serviceState}
        quitting={quitting}
      />,
    );
    const status = screen.getByRole("status");
    expect(status).toHaveAttribute("aria-live", "polite");
    expect(status).toHaveTextContent(label);
  });

  it("shows the active outage reason ahead of stale degraded state", () => {
    render(
      <ConnectionBanner
        connection="unavailable"
        serviceState="store_degraded"
        reason="The local server stopped responding."
      />,
    );

    const status = screen.getByRole("status");
    expect(status).toHaveTextContent("Server unavailable");
    expect(screen.getByText("The local server stopped responding.")).toHaveClass(
      "connection-detail",
    );
  });
});

describe("AppShell", () => {
  it("provides titled header, navigation, main, aside, and a polite connection status", () => {
    const fixture = agentFixture();
    render(<AppShell agent={fixture.result} />);

    expect(screen.getByRole("banner")).toBeVisible();
    expect(screen.getByRole("heading", { name: "NGY Coding Agent", level: 1 })).toBeVisible();
    expect(screen.getByRole("navigation", { name: "Repositories and tasks" })).toBeVisible();
    expect(screen.getByRole("main", { name: "Task workspace" })).toBeVisible();
    expect(screen.getByRole("complementary", { name: "Results and evidence" })).toBeVisible();
    expect(screen.getByRole("status")).toHaveAttribute("aria-live", "polite");
  });

  it("confirms explicit quit without registering a beforeunload handler", async () => {
    const user = userEvent.setup();
    const fixture = agentFixture();
    const addEventListener = vi.spyOn(window, "addEventListener");
    render(<AppShell agent={fixture.result} />);

    expect(addEventListener.mock.calls.some(([name]) => name === "beforeunload")).toBe(false);
    await user.click(screen.getByRole("button", { name: "Quit local application" }));
    const dialog = screen.getByRole("dialog", { name: "Quit local application?" });
    expect(within(dialog).getByText(/tasks continue when you only close/i)).toBeVisible();
    expect(within(dialog).getByRole("button", { name: "Keep running" })).toHaveFocus();
    await user.click(within(dialog).getByRole("button", { name: "Quit application" }));
    await waitFor(() => expect(fixture.result.quit).toHaveBeenCalledTimes(1));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Quit local application" }),
    ).toBeDisabled();
  });

  it("closes the quit dialog with Escape and restores trigger focus", async () => {
    const user = userEvent.setup();
    const fixture = agentFixture();
    render(<AppShell agent={fixture.result} />);
    const trigger = screen.getByRole("button", { name: "Quit local application" });

    trigger.focus();
    await user.keyboard("{Enter}");
    expect(screen.getByRole("dialog", { name: "Quit local application?" })).toBeVisible();
    await user.keyboard("{Escape}");

    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    await waitFor(() => expect(trigger).toHaveFocus());
    expect(fixture.result.quit).not.toHaveBeenCalled();
  });

  it("traps Tab navigation inside the modal quit dialog", async () => {
    const user = userEvent.setup();
    const fixture = agentFixture();
    render(<AppShell agent={fixture.result} />);
    await user.click(screen.getByRole("button", { name: "Quit local application" }));
    const dialog = screen.getByRole("dialog", { name: "Quit local application?" });
    const keepRunning = within(dialog).getByRole("button", { name: "Keep running" });
    const quit = within(dialog).getByRole("button", { name: "Quit application" });

    quit.focus();
    await user.keyboard("{Tab}");
    expect(keepRunning).toHaveFocus();
    await user.keyboard("{Shift>}{Tab}{/Shift}");
    expect(quit).toHaveFocus();
  });

  it("keeps focus in the dialog while the quit request is pending", async () => {
    const user = userEvent.setup();
    const fixture = agentFixture();
    fixture.result.quit = vi.fn(() => new Promise<never>(() => undefined));
    const { container } = render(<AppShell agent={fixture.result} />);
    await user.click(screen.getByRole("button", { name: "Quit local application" }));
    await user.click(screen.getByRole("button", { name: "Quit application" }));

    const dialogSurface = container.querySelector(".quit-dialog");
    await waitFor(() => expect(dialogSurface).toHaveFocus());
    expect(screen.getByRole("button", { name: "Keep running" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Shutting down…" })).toBeDisabled();
    await user.keyboard("{Tab}");
    expect(dialogSurface).toHaveFocus();
  });

  it("disables quit when the service is already quiescing", () => {
    const fixture = agentFixture({ serviceState: "quiescing" });
    render(<AppShell agent={fixture.result} />);

    expect(
      screen.getByRole("button", { name: "Quit local application" }),
    ).toBeDisabled();
    expect(screen.getByRole("status")).toHaveTextContent("Shutting down");
  });

  it("selects a newly created task and lets repository selection expose its empty state", async () => {
    const user = userEvent.setup();
    const fixture = agentFixture();
    render(<AppShell agent={fixture.result} />);

    await user.type(
      screen.getByRole("textbox", { name: "Task description" }),
      "  create from shell  ",
    );
    await user.click(screen.getByRole("button", { name: "Create task" }));
    await waitFor(() =>
      expect(fixture.result.newCreateTask).toHaveBeenCalledWith(
        "repo-new",
        "create from shell",
      ),
    );
    expect(fixture.result.selectTask).toHaveBeenCalledWith(fixture.created.id);

    await user.click(screen.getByRole("button", { name: /Older repository/ }));
    expect(screen.getByRole("heading", { name: "No task selected" })).toBeVisible();
  });
});

describe("ErrorBoundary", () => {
  it("isolates a failed panel behind a reusable fallback", () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    function Broken(): ReactNode {
      throw new Error("broken panel");
    }

    render(
      <ErrorBoundary fallback={<p role="alert">Panel could not be displayed.</p>}>
        <Broken />
      </ErrorBoundary>,
    );
    expect(screen.getByRole("alert")).toHaveTextContent("Panel could not be displayed.");
    consoleError.mockRestore();
  });
});
