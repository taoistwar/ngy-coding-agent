import { useId, useLayoutEffect, useRef, useState, type FormEvent } from "react";

import type { CreateTaskCommand } from "../api/client";
import type { Task } from "../api/types";

export interface TaskComposerProps {
  repositoryId: string | null;
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

const PROMPT_LIMIT = 50_000;

function scalarCount(value: string): number {
  return Array.from(value).length;
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
  onCreateTask,
  onCreated,
}: TaskComposerProps) {
  const [prompt, setPrompt] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [retryCommand, setRetryCommand] = useState<CreateTaskCommand | null>(null);
  const [failure, setFailure] = useState<ComposerFailure | null>(null);
  const contextRef = useRef({ repositoryId, version: 0 });
  const descriptionId = useId();
  const validationId = useId();
  const errorId = useId();
  const trimmedPrompt = prompt.trim();
  const normalizedCount = scalarCount(trimmedPrompt);
  const tooLong = normalizedCount > PROMPT_LIMIT;
  const valid = repositoryId !== null && normalizedCount > 0 && !tooLong;

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
  }, [repositoryId]);

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
    }
  };

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!valid || submitting) return;
    setSubmitting(true);
    setFailure(null);
    const submissionVersion = contextRef.current.version;
    let command = retryCommand;
    try {
      command ??= onCreateTask(repositoryId, trimmedPrompt);
      const created = await command.execute();
      if (contextRef.current.version === submissionVersion) {
        setRetryCommand(null);
        setPrompt("");
        onCreated(created);
      }
    } catch (error) {
      if (contextRef.current.version === submissionVersion) {
        const nextFailure = composerFailure(
          error,
          command?.clientRequestId ?? null,
        );
        setFailure(nextFailure);
        setRetryCommand(nextFailure.retryable ? command : null);
      }
    } finally {
      if (contextRef.current.version === submissionVersion) {
        setSubmitting(false);
      }
    }
  };

  const describedBy = [validationId, failure === null ? null : errorId]
    .filter((value): value is string => value !== null)
    .join(" ");

  return (
    <section className="task-composer" aria-labelledby="new-task-heading">
      <h2 id="new-task-heading">New task</h2>
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
            {failure.requestId !== null ? <p>Request ID: {failure.requestId}</p> : null}
            {failure.clientRequestId !== null ? (
              <p>Client request ID: {failure.clientRequestId}</p>
            ) : null}
          </div>
        ) : null}

        <button type="submit" disabled={!valid || submitting}>
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
