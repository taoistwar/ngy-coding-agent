import {
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
} from "react";

import type { Repository, Task } from "../api/types";

export interface SidebarProps {
  repositories: Repository[];
  tasks: Task[];
  selectedRepositoryId: string | null;
  selectedTaskId: string | null;
  onSelectRepository(repositoryId: string): void;
  onSelectTask(taskId: string): void;
  onAddRepository(path: string): Promise<Repository>;
  onPickRepository(): Promise<Repository | null>;
  onRetry(taskId: string): Promise<Task>;
}

interface SidebarError {
  message: string;
  requestId: string | null;
}

const STATUS_LABEL: Record<Task["status"], string> = {
  queued: "Queued",
  running: "Running",
  completed: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
  interrupted: "Interrupted",
};

const READINESS_LABEL: Record<Task["delivery_readiness"], string> = {
  unreviewed: "Unreviewed",
  review_approved: "Review approved",
  review_rejected: "Review rejected",
};

const RETRYABLE_STATUSES = new Set<Task["status"]>([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
]);

function timestamp(value: string): number {
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? 0 : parsed;
}

export function repositoryPathForDisplay(path: string): string {
  const verbatimPrefix = "\\\\?\\";
  if (!path.startsWith(verbatimPrefix)) return path;

  const remainder = path.slice(verbatimPrefix.length);
  const uncPrefix = "UNC\\";
  if (remainder.slice(0, uncPrefix.length).toUpperCase() === uncPrefix) {
    return `\\\\${remainder.slice(uncPrefix.length)}`;
  }
  return /^[A-Za-z]:[\\/]/u.test(remainder) ? remainder : path;
}

function sidebarError(error: unknown): SidebarError {
  if (typeof error !== "object" || error === null) {
    return { message: "The request failed.", requestId: null };
  }
  const message =
    "message" in error && typeof error.message === "string"
      ? error.message
      : "The request failed.";
  const requestId =
    "requestId" in error && typeof error.requestId === "string"
      ? error.requestId
      : null;
  return { message, requestId };
}

export function Sidebar({
  repositories,
  tasks,
  selectedRepositoryId,
  selectedTaskId,
  onSelectRepository,
  onSelectTask,
  onAddRepository,
  onPickRepository,
  onRetry,
}: SidebarProps) {
  const [path, setPath] = useState("");
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [error, setError] = useState<SidebarError | null>(null);
  const navigationVersionRef = useRef(0);
  const selectionRef = useRef({ selectedRepositoryId, selectedTaskId });
  useLayoutEffect(() => {
    if (
      selectionRef.current.selectedRepositoryId !== selectedRepositoryId ||
      selectionRef.current.selectedTaskId !== selectedTaskId
    ) {
      selectionRef.current = { selectedRepositoryId, selectedTaskId };
      navigationVersionRef.current += 1;
    }
  }, [selectedRepositoryId, selectedTaskId]);

  const selectRepository = (repositoryId: string) => {
    navigationVersionRef.current += 1;
    onSelectRepository(repositoryId);
  };

  const selectTask = (taskId: string) => {
    navigationVersionRef.current += 1;
    onSelectTask(taskId);
  };
  const sortedRepositories = useMemo(
    () =>
      [...repositories].sort(
        (left, right) =>
          timestamp(right.last_opened_at) - timestamp(left.last_opened_at) ||
          left.display_name.localeCompare(right.display_name),
      ),
    [repositories],
  );
  const visibleTasks = useMemo(
    () =>
      tasks
        .filter((value) => value.repository_id === selectedRepositoryId)
        .sort(
          (left, right) =>
            timestamp(right.created_at) - timestamp(left.created_at) ||
            right.attempt - left.attempt ||
            left.id.localeCompare(right.id),
        ),
    [selectedRepositoryId, tasks],
  );

  const addPath = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const normalizedPath = path.trim();
    if (normalizedPath.length === 0 || busyAction !== null) return;
    const navigationVersion = navigationVersionRef.current;
    setBusyAction("add");
    setError(null);
    try {
      const added = await onAddRepository(normalizedPath);
      setPath("");
      if (navigationVersionRef.current === navigationVersion) {
        selectRepository(added.id);
      }
    } catch (caught) {
      if (navigationVersionRef.current === navigationVersion) {
        setError(sidebarError(caught));
      }
    } finally {
      setBusyAction(null);
    }
  };

  const pickRepository = async () => {
    if (busyAction !== null) return;
    const navigationVersion = navigationVersionRef.current;
    setBusyAction("pick");
    setError(null);
    try {
      const picked = await onPickRepository();
      if (
        picked !== null &&
        navigationVersionRef.current === navigationVersion
      ) {
        selectRepository(picked.id);
      }
    } catch (caught) {
      if (navigationVersionRef.current === navigationVersion) {
        setError(sidebarError(caught));
      }
    } finally {
      setBusyAction(null);
    }
  };

  const retryTask = async (value: Task) => {
    if (busyAction !== null) return;
    const navigationVersion = navigationVersionRef.current;
    setBusyAction(`retry:${value.id}`);
    setError(null);
    try {
      const retried = await onRetry(value.id);
      if (navigationVersionRef.current === navigationVersion) {
        selectTask(retried.id);
      }
    } catch (caught) {
      if (navigationVersionRef.current === navigationVersion) {
        setError(sidebarError(caught));
      }
    } finally {
      setBusyAction(null);
    }
  };

  return (
    <nav className="sidebar" aria-label="Repositories and tasks">
      <section aria-labelledby="repositories-heading">
        <h2 id="repositories-heading">Repositories</h2>
        <form className="repository-path-form" onSubmit={(event) => void addPath(event)}>
          <label htmlFor="repository-path">Repository path</label>
          <div className="inline-control">
            <input
              id="repository-path"
              name="repository-path"
              type="text"
              value={path}
              onChange={(event) => setPath(event.currentTarget.value)}
              disabled={busyAction !== null}
              autoComplete="off"
            />
            <button
              type="submit"
              disabled={path.trim().length === 0 || busyAction !== null}
            >
              {busyAction === "add" ? "Adding…" : "Add repository path"}
            </button>
          </div>
        </form>
        <button
          type="button"
          onClick={() => void pickRepository()}
          disabled={busyAction !== null}
        >
          {busyAction === "pick" ? "Opening picker…" : "Choose repository folder"}
        </button>

        {sortedRepositories.length === 0 ? (
          <p className="empty-state">No repositories yet.</p>
        ) : (
          <ul className="repository-list" aria-label="Repositories">
            {sortedRepositories.map((repository) => (
              <li key={repository.id}>
                <button
                  type="button"
                  className="repository-button"
                  aria-current={
                    repository.id === selectedRepositoryId ? "page" : undefined
                  }
                  onClick={() => selectRepository(repository.id)}
                >
                  <strong>{repository.display_name}</strong>
                  <span>{repositoryPathForDisplay(repository.selected_path)}</span>
                </button>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section aria-labelledby="tasks-heading">
        <h2 id="tasks-heading">Tasks</h2>
        {selectedRepositoryId === null ? (
          <p className="empty-state">Add or choose a repository to get started.</p>
        ) : visibleTasks.length === 0 ? (
          <p className="empty-state">No tasks yet for this repository.</p>
        ) : (
          <ul className="task-list" aria-label="Tasks">
            {visibleTasks.map((value) => (
              <li key={value.id} className="task-list-item">
                <button
                  type="button"
                  className="task-button"
                  aria-current={value.id === selectedTaskId ? "page" : undefined}
                  onClick={() => selectTask(value.id)}
                >
                  <strong>{value.prompt}</strong>
                  <span>Attempt {value.attempt}</span>
                </button>
                <span className="task-list-badges">
                  <span className={`task-list-status status-${value.status}`}>
                    <span aria-hidden="true">
                      {value.status === "completed" ? "✓" : "◆"}
                    </span>{" "}
                    {STATUS_LABEL[value.status]}
                  </span>
                  <span
                    className={`task-list-readiness readiness-${value.delivery_readiness}`}
                  >
                    {READINESS_LABEL[value.delivery_readiness]}
                  </span>
                </span>
                {RETRYABLE_STATUSES.has(value.status) &&
                !tasks.some((candidate) => candidate.retry_of === value.id) ? (
                  <button
                    type="button"
                    className="retry-button"
                    aria-label={`Retry task ${value.prompt}`}
                    disabled={busyAction !== null}
                    onClick={() => void retryTask(value)}
                  >
                    {busyAction === `retry:${value.id}` ? "Retrying…" : "Retry"}
                  </button>
                ) : null}
              </li>
            ))}
          </ul>
        )}
      </section>

      {error !== null ? (
        <div className="sidebar-error" role="alert">
          <p>{error.message}</p>
          {error.requestId !== null ? <p>Request ID: {error.requestId}</p> : null}
        </div>
      ) : null}
    </nav>
  );
}
