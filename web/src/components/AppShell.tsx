import { useEffect, useMemo, useRef, useState } from "react";

import type { Repository, Task } from "../api/types";
import type { UseAgentStateResult } from "../state/useAgentState";
import { ConnectionBanner } from "./ConnectionBanner";
import { ResizableWorkbench } from "./ResizableWorkbench";
import { Sidebar } from "./Sidebar";
import { TaskComposer } from "./TaskComposer";
import { TaskWorkspace } from "./TaskWorkspace";

export interface AppShellProps {
  agent: UseAgentStateResult;
}

interface QuitFailure {
  message: string;
  requestId: string | null;
}

function orderedValues<T>(order: string[], values: Record<string, T>): T[] {
  const seen = new Set<string>();
  const ordered: T[] = [];
  for (const id of order) {
    const value = values[id];
    if (value !== undefined && !seen.has(id)) {
      seen.add(id);
      ordered.push(value);
    }
  }
  for (const [id, value] of Object.entries(values)) {
    if (!seen.has(id)) ordered.push(value);
  }
  return ordered;
}

function quitFailure(error: unknown): QuitFailure {
  if (typeof error !== "object" || error === null) {
    return { message: "The local application could not be stopped.", requestId: null };
  }
  return {
    message:
      "message" in error && typeof error.message === "string"
        ? error.message
        : "The local application could not be stopped.",
    requestId:
      "requestId" in error && typeof error.requestId === "string"
        ? error.requestId
        : null,
  };
}

export function AppShell({ agent }: AppShellProps) {
  const repositories = useMemo(
    () => orderedValues(agent.state.repositoryOrder, agent.state.repositoriesById),
    [agent.state.repositoriesById, agent.state.repositoryOrder],
  );
  const tasks = useMemo(
    () => orderedValues(agent.state.taskOrder, agent.state.tasksById),
    [agent.state.taskOrder, agent.state.tasksById],
  );
  const selectedStateTask =
    agent.state.selectedTaskId === null
      ? null
      : (agent.state.tasksById[agent.state.selectedTaskId] ?? null);
  const [selectedRepositoryId, setSelectedRepositoryId] = useState<string | null>(
    () => selectedStateTask?.repository_id ?? repositories[0]?.id ?? null,
  );
  const [quitDialogOpen, setQuitDialogOpen] = useState(false);
  const [quitting, setQuitting] = useState(false);
  const [quitError, setQuitError] = useState<QuitFailure | null>(null);
  const quitTriggerRef = useRef<HTMLButtonElement>(null);
  const keepRunningRef = useRef<HTMLButtonElement>(null);
  const quitDialogRef = useRef<HTMLElement>(null);
  const shuttingDown = quitting || agent.state.serviceState === "quiescing";

  useEffect(() => {
    if (
      selectedRepositoryId !== null &&
      agent.state.repositoriesById[selectedRepositoryId] !== undefined
    ) {
      return;
    }
    setSelectedRepositoryId(selectedStateTask?.repository_id ?? repositories[0]?.id ?? null);
  }, [
    agent.state.repositoriesById,
    repositories,
    selectedRepositoryId,
    selectedStateTask?.repository_id,
  ]);

  useEffect(() => {
    if (selectedStateTask !== null) {
      setSelectedRepositoryId(selectedStateTask.repository_id);
    }
  }, [agent.state.selectedTaskId, selectedStateTask?.repository_id]);

  useEffect(() => {
    if (!quitDialogOpen) return;
    if (shuttingDown) {
      quitDialogRef.current?.focus();
    } else {
      keepRunningRef.current?.focus();
    }
  }, [quitDialogOpen, shuttingDown]);

  const repository: Repository | null =
    selectedRepositoryId === null
      ? null
      : (agent.state.repositoriesById[selectedRepositoryId] ?? null);
  const task: Task | null =
    selectedStateTask !== null &&
    selectedStateTask.repository_id === selectedRepositoryId
      ? selectedStateTask
      : null;
  const detail =
    task !== null && agent.state.selectedDetail?.task.id === task.id
      ? agent.state.selectedDetail
      : null;

  const selectTask = (taskId: string) => {
    const next = agent.state.tasksById[taskId];
    if (next !== undefined) setSelectedRepositoryId(next.repository_id);
    agent.selectTask(taskId);
  };

  const confirmQuit = async () => {
    if (shuttingDown) return;
    setQuitting(true);
    setQuitError(null);
    try {
      await agent.quit();
      setQuitDialogOpen(false);
    } catch (error) {
      setQuitting(false);
      setQuitError(quitFailure(error));
    }
  };

  const closeQuitDialog = () => {
    setQuitDialogOpen(false);
    queueMicrotask(() => quitTriggerRef.current?.focus());
  };

  return (
    <div className="app-shell">
      <header
        className="app-header"
        inert={quitDialogOpen ? true : undefined}
      >
        <div>
          <p className="eyebrow">Local coding workbench</p>
          <h1>NGY Coding Agent</h1>
        </div>
        <ConnectionBanner
          connection={agent.state.connection}
          serviceState={agent.state.serviceState}
          quitting={shuttingDown}
          reason={agent.state.recoveryReason}
        />
        <button
          ref={quitTriggerRef}
          type="button"
          className="quit-button"
          aria-haspopup="dialog"
          disabled={shuttingDown}
          onClick={() => {
            setQuitError(null);
            setQuitDialogOpen(true);
          }}
        >
          Quit local application
        </button>
      </header>

      <ResizableWorkbench inert={quitDialogOpen}>
        <Sidebar
          repositories={repositories}
          tasks={tasks}
          selectedRepositoryId={selectedRepositoryId}
          selectedTaskId={task?.id ?? null}
          onSelectRepository={setSelectedRepositoryId}
          onSelectTask={selectTask}
          onAddRepository={agent.addRepository}
          onPickRepository={agent.pickRepository}
          onRetry={agent.retryTask}
        />
        <TaskWorkspace
          task={task}
          detail={detail}
          detailLoading={task !== null && agent.state.detailLoading}
          detailError={task === null ? null : agent.state.detailError}
          cancelState={
            task === null ? undefined : agent.state.commands.cancelByTaskId[task.id]
          }
          tasksById={agent.state.tasksById}
          taskOrder={agent.state.taskOrder}
          onCancel={agent.cancelTask}
          onRetry={agent.retryTask}
          onSelectTask={selectTask}
          repository={repository}
          composer={
            <TaskComposer
              repositoryId={selectedRepositoryId}
              onCreateTask={agent.newCreateTask}
              onCreated={(created) => {
                setSelectedRepositoryId(created.repository_id);
                agent.selectTask(created.id);
              }}
            />
          }
        />
      </ResizableWorkbench>

      {quitDialogOpen ? (
        <div
          className="modal-backdrop"
          role="dialog"
          aria-modal="true"
          aria-labelledby="quit-dialog-heading"
          aria-describedby="quit-dialog-description"
          onKeyDown={(event) => {
            if (event.key === "Escape" && !shuttingDown) {
              event.preventDefault();
              closeQuitDialog();
              return;
            }
            if (event.key === "Tab") {
              const buttons = Array.from(
                quitDialogRef.current?.querySelectorAll<HTMLButtonElement>(
                  "button:not(:disabled)",
                ) ?? [],
              );
              if (buttons.length === 0) {
                event.preventDefault();
                quitDialogRef.current?.focus();
                return;
              }
              const first = buttons[0];
              const last = buttons[buttons.length - 1];
              if (
                (event.shiftKey && document.activeElement === first) ||
                (!event.shiftKey && document.activeElement === last) ||
                !quitDialogRef.current?.contains(document.activeElement)
              ) {
                event.preventDefault();
                (event.shiftKey ? last : first)?.focus();
              }
            }
          }}
        >
          <section ref={quitDialogRef} className="quit-dialog" tabIndex={-1}>
            <h2 id="quit-dialog-heading">Quit local application?</h2>
            <p id="quit-dialog-description">
              Tasks continue when you only close this browser tab. Quitting stops the
              local service and safely interrupts unfinished tasks.
            </p>
            {quitError !== null ? (
              <div role="alert" className="quit-error">
                <p>{quitError.message}</p>
                {quitError.requestId !== null ? (
                  <p>Request ID: {quitError.requestId}</p>
                ) : null}
              </div>
            ) : null}
            <div className="dialog-actions">
              <button
                ref={keepRunningRef}
                type="button"
                disabled={shuttingDown}
                onClick={closeQuitDialog}
              >
                Keep running
              </button>
              <button
                type="button"
                disabled={shuttingDown}
                onClick={() => void confirmQuit()}
              >
                {shuttingDown ? "Shutting down…" : "Quit application"}
              </button>
            </div>
          </section>
        </div>
      ) : null}
    </div>
  );
}
