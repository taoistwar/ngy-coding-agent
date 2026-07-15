import { useRef, useState, type ReactNode } from "react";

import type { Repository, Task, TaskDetail } from "../api/types";
import type { CancelCommandState } from "../state/model";
import { ActivityPane } from "./ActivityPane";
import { ErrorBoundary } from "./ErrorBoundary";
import { PlanPane } from "./PlanPane";
import { ResultPane } from "./ResultPane";

export interface TaskWorkspaceProps {
  task: Task | null;
  detail: TaskDetail | null;
  detailLoading: boolean;
  detailError: string | null;
  cancelState: CancelCommandState | undefined;
  tasksById: Record<string, Task>;
  taskOrder: string[];
  onCancel: (taskId: string) => void | Task | Promise<void | Task>;
  onRetry: (taskId: string) => Task | void | Promise<Task | void>;
  onSelectTask: (taskId: string) => void;
  repository?: Repository | null;
  composer?: ReactNode;
}

const STATUS_GLYPH: Record<Task["status"], string> = {
  queued: "○",
  running: "◐",
  completed: "✓",
  failed: "×",
  cancelled: "−",
  interrupted: "!",
};

const TERMINAL_STATUSES = new Set<Task["status"]>([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
]);

interface RetryFailure {
  code: string | null;
  message: string;
  requestId: string | null;
}

type RetryCommandViewState =
  | { phase: "pending" }
  | { phase: "error"; error: RetryFailure };

function retryFailure(error: unknown): RetryFailure {
  if (typeof error !== "object" || error === null) {
    return {
      code: null,
      message: "The retry attempt could not be created.",
      requestId: null,
    };
  }
  return {
    code: "code" in error && typeof error.code === "string" ? error.code : null,
    message:
      "message" in error && typeof error.message === "string"
        ? error.message
        : "The retry attempt could not be created.",
    requestId:
      "requestId" in error && typeof error.requestId === "string"
        ? error.requestId
        : null,
  };
}

function retryChain(task: Task, tasksById: Record<string, Task>, taskOrder: string[]) {
  const orderIndex = new Map(taskOrder.map((id, index) => [id, index]));
  const seenAncestors = new Set<string>();
  let root = task;
  while (root.retry_of !== null && root.retry_of !== undefined) {
    if (seenAncestors.has(root.id)) break;
    seenAncestors.add(root.id);
    const previous = tasksById[root.retry_of];
    if (previous === undefined) break;
    root = previous;
  }

  const chain: Task[] = [];
  const seen = new Set<string>();
  let current: Task | undefined = root;
  while (current !== undefined && !seen.has(current.id)) {
    chain.push(current);
    seen.add(current.id);
    current = Object.values(tasksById)
      .filter((candidate) => candidate.retry_of === current?.id && !seen.has(candidate.id))
      .sort(
        (left, right) =>
          left.attempt - right.attempt ||
          (orderIndex.get(left.id) ?? Number.MAX_SAFE_INTEGER) -
            (orderIndex.get(right.id) ?? Number.MAX_SAFE_INTEGER),
      )[0];
  }
  return chain;
}

function AttemptChain({
  task,
  tasksById,
  taskOrder,
  onSelectTask,
}: Pick<TaskWorkspaceProps, "task" | "tasksById" | "taskOrder" | "onSelectTask"> & {
  task: Task;
}) {
  const chain = retryChain(task, tasksById, taskOrder);
  return (
    <section className="attempt-chain" aria-labelledby="attempt-chain-heading">
      <h3 id="attempt-chain-heading">Attempts</h3>
      <ol>
        {chain.map((attempt) => (
          <li key={attempt.id}>
            <button
              type="button"
              className={attempt.id === task.id ? "attempt-link selected" : "attempt-link"}
              aria-current={attempt.id === task.id ? "page" : undefined}
              onClick={() => onSelectTask(attempt.id)}
            >
              Attempt {attempt.attempt} — {attempt.status}
            </button>
          </li>
        ))}
      </ol>
    </section>
  );
}

export function TaskWorkspace({
  task,
  detail,
  detailLoading,
  detailError,
  cancelState,
  tasksById,
  taskOrder,
  onCancel,
  onRetry,
  onSelectTask,
  repository = null,
  composer = null,
}: TaskWorkspaceProps) {
  const [retryByTaskId, setRetryByTaskId] = useState<
    Record<string, RetryCommandViewState>
  >({});
  const selectedTaskIdRef = useRef<string | null>(task?.id ?? null);
  selectedTaskIdRef.current = task?.id ?? null;

  if (task === null) {
    return (
      <>
        <main className="workspace-pane" aria-labelledby="workspace-heading">
          {composer}
          <div className="workspace-empty">
            <h2 id="workspace-heading">Task workspace</h2>
            <h3>No task selected</h3>
            <p>Choose a task from the sidebar to inspect its execution.</p>
          </div>
        </main>
        <aside className="result-pane" aria-labelledby="results-heading">
          <h2 id="results-heading">Results and evidence</h2>
          <p className="empty-state">Select a task to inspect its evidence.</p>
        </aside>
      </>
    );
  }

  const canCancel = task.status === "queued" || task.status === "running";
  const cancelPending = canCancel && cancelState?.phase === "pending";
  const hasNewerAttempt = Object.values(tasksById).some(
    (candidate) => candidate.retry_of === task.id,
  );
  const canRetry = TERMINAL_STATUSES.has(task.status) && !hasNewerAttempt;
  const retryState = retryByTaskId[task.id];
  const retryPending = retryState?.phase === "pending";

  const handleCancel = () => {
    void Promise.resolve(onCancel(task.id)).catch(() => undefined);
  };

  const handleRetry = async () => {
    if (retryPending) return;
    const sourceTaskId = task.id;
    setRetryByTaskId((current) => ({
      ...current,
      [sourceTaskId]: { phase: "pending" },
    }));
    try {
      const nextTask = await onRetry(sourceTaskId);
      setRetryByTaskId((current) => {
        if (current[sourceTaskId]?.phase !== "pending") return current;
        const next = { ...current };
        delete next[sourceTaskId];
        return next;
      });
      if (
        nextTask !== undefined &&
        selectedTaskIdRef.current === sourceTaskId
      ) {
        onSelectTask(nextTask.id);
      }
    } catch (error) {
      setRetryByTaskId((current) => ({
        ...current,
        [sourceTaskId]: { phase: "error", error: retryFailure(error) },
      }));
    }
  };

  return (
    <>
      <main className="workspace-pane" aria-labelledby="workspace-heading">
        {composer}
        <div className="task-heading">
          <div>
            <h2 id="workspace-heading">Task workspace</h2>
            <p className="eyebrow">
              {repository?.display_name ?? "Task execution"} · Attempt {task.attempt}
            </p>
            <h3>{task.prompt}</h3>
            <p className={`task-status status-${task.status}`}>
              <span className="status-glyph" aria-hidden="true">
                {STATUS_GLYPH[task.status]}
              </span>{" "}
              Status: {task.status}
            </p>
            {task.status === "completed" ? (
              <p className="completion-disclaimer">Execution completed — not reviewed</p>
            ) : null}
          </div>
          <div className="task-actions" aria-label="Task actions">
            {canCancel ? (
              <button
                type="button"
                onClick={handleCancel}
                disabled={cancelPending}
                aria-label={cancelPending ? "Cancelling" : "Cancel task"}
              >
                {cancelPending ? "Cancelling" : "Cancel task"}
              </button>
            ) : null}
            {canRetry ? (
              <button
                type="button"
                onClick={() => void handleRetry()}
                disabled={retryPending}
              >
                {retryPending ? "Retrying" : "Retry task"}
              </button>
            ) : null}
            {hasNewerAttempt ? <span className="readonly-badge">Read-only attempt</span> : null}
          </div>
        </div>

        {cancelState?.phase === "error" && canCancel ? (
          <div className="command-error" role="alert">
            <p>Cancel failed: {cancelState.error.message}</p>
            {cancelState.error.requestId !== null ? (
              <p>Request ID: {cancelState.error.requestId}</p>
            ) : null}
          </div>
        ) : null}
        {retryState?.phase === "error" ? (
          <div className="command-error" role="alert">
            <p>Retry failed: {retryState.error.message}</p>
            {retryState.error.code !== null ? (
              <p>Error code: {retryState.error.code}</p>
            ) : null}
            {retryState.error.requestId !== null ? (
              <p>Request ID: {retryState.error.requestId}</p>
            ) : null}
          </div>
        ) : null}

        {detailLoading ? (
          <p role="status" aria-label="Loading task details">
            Loading task details…
          </p>
        ) : null}
        {detailError !== null ? <p role="alert">{detailError}</p> : null}

        {detail !== null ? (
          <div className="center-evidence">
            <ErrorBoundary
              fallback={<p role="alert">Plan unavailable</p>}
              resetKey={`${task.id}:${detail.event_cursor}`}
            >
              <PlanPane plan={detail.plan ?? null} />
            </ErrorBoundary>
            <ErrorBoundary
              fallback={<p role="alert">Activity unavailable</p>}
              resetKey={`${task.id}:${detail.event_cursor}`}
            >
              <ActivityPane activity={detail.activity} />
            </ErrorBoundary>
          </div>
        ) : !detailLoading && detailError === null ? (
          <p className="empty-state">Task details have not loaded yet.</p>
        ) : null}
      </main>

      <aside className="result-pane" aria-labelledby="results-heading">
        <AttemptChain
          task={task}
          tasksById={tasksById}
          taskOrder={taskOrder}
          onSelectTask={onSelectTask}
        />
        {detail !== null ? (
          <ResultPane
            task={task}
            diff={detail.diff ?? null}
            tests={detail.tests ?? null}
            timeline={detail.timeline}
            boundaryResetKey={`${task.id}:${detail.event_cursor}`}
          />
        ) : (
          <>
            <h2 id="results-heading">Results and evidence</h2>
            <p className="empty-state">
              {detailLoading
                ? "Loading task evidence…"
                : detailError !== null
                  ? "Task evidence is unavailable."
                  : "No task evidence is available yet."}
            </p>
          </>
        )}
      </aside>
    </>
  );
}
