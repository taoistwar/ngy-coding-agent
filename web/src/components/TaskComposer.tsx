import { useId, useLayoutEffect, useRef, useState, type FormEvent } from "react";

import type { CreateTaskCommand } from "../api/client";
import type { Task } from "../api/types";
import type { QueueFullReplayState } from "../state/model";
import type { SchedulerProjectionState } from "../state/schedulerProjection";

export interface TaskComposerProps {
  repositoryId: string | null;
  scheduler: SchedulerProjectionState;
  queueFullReplay: QueueFullReplayState | null;
  onCreateTask(repositoryId: string, prompt: string): CreateTaskCommand;
  onCreated(task: Task): void;
}

interface ComposerFailure {
  code: string | null;
  message: string;
  requestId: string | null;
  retryable: boolean;
  clientRequestId: string | null;
}

interface SchedulerVersion {
  serverInstanceId: string;
  generation: number;
}

interface RetainedQueueFullCommand {
  command: CreateTaskCommand;
  failure: ComposerFailure;
  boundary: SchedulerVersion | null;
}

const PROMPT_LIMIT = 50_000;
const TASK_QUEUE_FULL = "TASK_QUEUE_FULL";
const AMBIGUOUS_CREATE_CODES = new Set([
  "NETWORK_ERROR",
  "APP_SHUTTING_DOWN",
]);

function scalarCount(value: string): number {
  return Array.from(value).length;
}

function freshQueueCapacity(
  scheduler: SchedulerProjectionState,
): "available" | "full" | "unknown" {
  if (scheduler.freshness !== "fresh" || scheduler.snapshot === null) {
    return "unknown";
  }
  return scheduler.snapshot.queued_task_count >= scheduler.snapshot.limits.queued
    ? "full"
    : "available";
}

function schedulerVersion(
  scheduler: SchedulerProjectionState,
): SchedulerVersion | null {
  if (scheduler.snapshot === null) return null;
  return {
    serverInstanceId: scheduler.snapshot.server_instance_id,
    generation: scheduler.snapshot.generation,
  };
}

function isLaterProjection(
  scheduler: SchedulerProjectionState,
  boundary: SchedulerVersion | null,
): boolean {
  if (scheduler.freshness !== "fresh") return false;
  const current = schedulerVersion(scheduler);
  if (current === null) return false;
  if (boundary === null) return true;
  return (
    current.serverInstanceId !== boundary.serverInstanceId ||
    current.generation > boundary.generation
  );
}

function composerFailure(
  error: unknown,
  clientRequestId: string | null,
): ComposerFailure {
  if (typeof error !== "object" || error === null) {
    return {
      code: null,
      message: "The task could not be created.",
      requestId: null,
      retryable: false,
      clientRequestId,
    };
  }
  return {
    code: "code" in error && typeof error.code === "string" ? error.code : null,
    message:
      "message" in error && typeof error.message === "string"
        ? error.message
        : "The task could not be created.",
    requestId:
      "requestId" in error && typeof error.requestId === "string"
        ? error.requestId
        : null,
    retryable:
      "retryable" in error && typeof error.retryable === "boolean"
        ? error.retryable
        : false,
    clientRequestId,
  };
}

export function TaskComposer({
  repositoryId,
  scheduler,
  queueFullReplay,
  onCreateTask,
  onCreated,
}: TaskComposerProps) {
  const [prompt, setPrompt] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [retryCommand, setRetryCommand] = useState<CreateTaskCommand | null>(null);
  const [failure, setFailure] = useState<ComposerFailure | null>(null);
  const [queueFullBoundary, setQueueFullBoundary] =
    useState<SchedulerVersion | null>(null);
  const retainedQueueFullRef = useRef<RetainedQueueFullCommand | null>(null);
  const contextRef = useRef({ repositoryId, version: 0 });
  const descriptionId = useId();
  const validationId = useId();
  const errorId = useId();
  const capacityId = useId();
  const trimmedPrompt = prompt.trim();
  const normalizedCount = scalarCount(trimmedPrompt);
  const tooLong = normalizedCount > PROMPT_LIMIT;
  const valid = repositoryId !== null && normalizedCount > 0 && !tooLong;
  const replayMatches =
    repositoryId !== null &&
    retryCommand !== null &&
    queueFullReplay !== null &&
    queueFullReplay.repositoryId === repositoryId &&
    queueFullReplay.prompt === trimmedPrompt &&
    queueFullReplay.clientRequestId === retryCommand.clientRequestId;
  const queueFullRetry =
    retryCommand !== null &&
    (failure?.code === TASK_QUEUE_FULL || replayMatches);
  const ambiguousRetry =
    retryCommand !== null &&
    failure !== null &&
    failure.code !== null &&
    AMBIGUOUS_CREATE_CODES.has(failure.code);
  const capacity = freshQueueCapacity(scheduler);
  const queueFullRetryReady =
    queueFullRetry &&
    capacity === "available" &&
    isLaterProjection(scheduler, queueFullBoundary);
  const queueFullRetryBlocked = queueFullRetry && !queueFullRetryReady;
  const freshCapacityBlocked =
    !queueFullRetry && !ambiguousRetry && capacity === "full";
  const capacityBlocked = freshCapacityBlocked || queueFullRetryBlocked;
  const canSubmit = valid && !submitting && !capacityBlocked;

  useLayoutEffect(() => {
    if (contextRef.current.repositoryId !== repositoryId) {
      contextRef.current = {
        repositoryId,
        version: contextRef.current.version + 1,
      };
    }
    setSubmitting(false);
    setRetryCommand(null);
    setFailure(null);
    setQueueFullBoundary(null);
    const retained = retainedQueueFullRef.current;
    if (
      repositoryId !== null &&
      queueFullReplay !== null &&
      queueFullReplay.repositoryId === repositoryId &&
      retained?.command.clientRequestId === queueFullReplay.clientRequestId
    ) {
      setPrompt(queueFullReplay.prompt);
      setRetryCommand(retained.command);
      setFailure(retained.failure);
      setQueueFullBoundary(retained.boundary);
    }
  }, [queueFullReplay, repositoryId]);

  if (repositoryId === null) {
    return (
      <section className="task-composer" aria-labelledby="new-task-heading">
        <h2 id="new-task-heading">New task</h2>
        <p className="empty-state">Select a repository before creating a task.</p>
      </section>
    );
  }

  const updatePrompt = (value: string) => {
    contextRef.current = {
      repositoryId: contextRef.current.repositoryId,
      version: contextRef.current.version + 1,
    };
    setPrompt(value);
    if (retryCommand !== null || failure !== null) {
      setRetryCommand(null);
      setFailure(null);
      setQueueFullBoundary(null);
    }
  };

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!canSubmit) return;
    setSubmitting(true);
    setFailure(null);
    const submissionVersion = contextRef.current.version;
    const submissionSchedulerVersion = schedulerVersion(scheduler);
    let command = retryCommand;
    try {
      command ??= onCreateTask(repositoryId, trimmedPrompt);
      const created = await command.execute();
      if (
        retainedQueueFullRef.current?.command.clientRequestId ===
        command.clientRequestId
      ) {
        retainedQueueFullRef.current = null;
      }
      if (contextRef.current.version === submissionVersion) {
        setRetryCommand(null);
        setQueueFullBoundary(null);
        setPrompt("");
        onCreated(created);
      }
    } catch (error) {
      const nextFailure = composerFailure(
        error,
        command?.clientRequestId ?? null,
      );
      const nextBoundary =
        nextFailure.code === TASK_QUEUE_FULL
          ? submissionSchedulerVersion
          : null;
      if (nextFailure.code === TASK_QUEUE_FULL && command !== null) {
        retainedQueueFullRef.current = {
          command,
          failure: nextFailure,
          boundary: nextBoundary,
        };
      }
      if (contextRef.current.version === submissionVersion) {
        setFailure(nextFailure);
        setRetryCommand(nextFailure.retryable ? command : null);
        setQueueFullBoundary(nextBoundary);
      }
    } finally {
      if (contextRef.current.version === submissionVersion) {
        setSubmitting(false);
      }
    }
  };

  const capacityMessage = queueFullRetryBlocked
    ? "This exact request can be retried when a fresh scheduler snapshot shows available queue capacity."
    : freshCapacityBlocked
      ? "Task queue capacity is full. New task submission is disabled until capacity becomes available."
      : null;
  const describedBy = [
    validationId,
    failure === null ? null : errorId,
    capacityMessage === null ? null : capacityId,
  ]
    .filter((value): value is string => value !== null)
    .join(" ");
  const failureRequestId =
    replayMatches && queueFullReplay.requestId !== null
      ? queueFullReplay.requestId
      : failure?.requestId ?? null;
  const failureClientRequestId = replayMatches
    ? queueFullReplay.clientRequestId
    : failure?.clientRequestId ?? null;

  return (
    <section className="task-composer" aria-labelledby="new-task-heading">
      <h2 id="new-task-heading">New task</h2>
      <p className="execution-boundary-note">
        Tasks run in an isolated Git worktree. Cargo executes repository and
        generated code with your current user permissions; this is not a
        malicious-code sandbox.
      </p>
      <form onSubmit={(event) => void submit(event)} noValidate>
        <label htmlFor={descriptionId}>Task description</label>
        <textarea
          id={descriptionId}
          value={prompt}
          onChange={(event) => updatePrompt(event.currentTarget.value)}
          aria-describedby={describedBy}
          aria-invalid={tooLong || (prompt.length > 0 && trimmedPrompt.length === 0)}
          disabled={submitting}
          rows={4}
        />
        <div className="composer-meta" id={validationId}>
          <span>{normalizedCount.toLocaleString("en-US")} / 50,000 characters</span>
          {tooLong ? (
            <span className="validation-error">
              Task descriptions must be 50,000 characters or fewer.
            </span>
          ) : null}
        </div>

        {failure !== null ? (
          <div id={errorId} className="composer-error" role="alert">
            <p>{failure.message}</p>
            {failure.code !== null ? <p>Error code: {failure.code}</p> : null}
            {failureRequestId !== null ? <p>Request ID: {failureRequestId}</p> : null}
            {failureClientRequestId !== null ? (
              <p>Client request ID: {failureClientRequestId}</p>
            ) : null}
          </div>
        ) : null}

        {capacityMessage !== null ? (
          <p id={capacityId} className="queue-capacity-note">
            {capacityMessage}
          </p>
        ) : null}

        <button
          type="submit"
          disabled={!canSubmit}
          aria-describedby={capacityMessage === null ? undefined : capacityId}
        >
          {submitting
            ? "Creating…"
            : retryCommand !== null
              ? "Retry create task"
              : "Create task"}
        </button>
      </form>
    </section>
  );
}
