import { useCallback, useEffect, useReducer, useRef } from "react";

import type { CreateTaskCommand } from "../api/client";
import { SseClient } from "../api/sse";
import type { SseClientOptions, SseClientState } from "../api/sse";
import type {
  BootstrapResponse,
  CancellationAcceptedResponse,
  QuitResponse,
  Repository,
  Task,
  TaskDetail,
  TaskEvent,
} from "../api/types";
import { initialAgentState, type AgentConnectionState, type AgentState } from "./model";
import { agentReducer } from "./reducer";

export interface AgentApiAdapter {
  initialize(): Promise<BootstrapResponse>;
  bootstrap(signal?: AbortSignal): Promise<BootstrapResponse>;
  addRepository(path: string): Promise<Repository>;
  pickRepository(): Promise<Repository | null>;
  newCreateTask(repositoryId: string, prompt: string): CreateTaskCommand;
  taskDetail(taskId: string): Promise<TaskDetail>;
  cancelTask(taskId: string): Promise<CancellationAcceptedResponse>;
  retryTask(taskId: string): Promise<Task>;
  quit(): Promise<QuitResponse>;
}

export interface AgentStreamCallbacks {
  onTaskEvent(event: TaskEvent): void;
  onUnknownEvent(id: number, kind: string, schemaVersion: number): void;
  onServiceState(
    state: BootstrapResponse["service_state"],
    generation: number,
  ): void;
  onBootstrap(bootstrap: BootstrapResponse): void;
  onConnectionState(connection: AgentConnectionState, reason?: string): void;
  onSessionExpired(): void;
}

export interface AgentStreamAdapter {
  start(cursor: number): void | Promise<void>;
  stop(): void;
}

export interface UseAgentStateDependencies {
  api: AgentApiAdapter;
  createStream?(callbacks: AgentStreamCallbacks): AgentStreamAdapter;
}

export interface UseAgentStateResult {
  state: AgentState;
  selectTask(taskId: string): void;
  addRepository(path: string): Promise<Repository>;
  pickRepository(): Promise<Repository | null>;
  newCreateTask(repositoryId: string, prompt: string): CreateTaskCommand;
  cancelTask(taskId: string): Promise<Task>;
  retryTask(taskId: string): Promise<Task>;
  quit(): Promise<QuitResponse>;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  if (
    typeof error === "object" &&
    error !== null &&
    "message" in error &&
    typeof error.message === "string"
  ) {
    return error.message;
  }
  return "The request failed.";
}

function commandError(error: unknown): {
  code: string;
  message: string;
  retryable: boolean;
  requestId: string | null;
} {
  if (typeof error !== "object" || error === null) {
    return {
      code: "REQUEST_FAILED",
      message: errorMessage(error),
      retryable: false,
      requestId: null,
    };
  }
  const code = "code" in error && typeof error.code === "string" ? error.code : "REQUEST_FAILED";
  const retryable =
    "retryable" in error && typeof error.retryable === "boolean"
      ? error.retryable
      : false;
  const requestId =
    "requestId" in error && typeof error.requestId === "string"
      ? error.requestId
      : null;
  return { code, message: errorMessage(error), retryable, requestId };
}

function isSessionExpired(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "status" in error &&
    error.status === 401
  );
}

function projectSseState(
  state: SseClientState,
  callbacks: AgentStreamCallbacks,
): void {
  switch (state.kind) {
    case "connecting":
      callbacks.onConnectionState("reconnecting");
      break;
    case "open":
      callbacks.onConnectionState("live");
      break;
    case "reconnecting":
      callbacks.onConnectionState("reconnecting", state.reason);
      break;
    case "recovering":
      callbacks.onConnectionState("recovering", state.reason.message);
      break;
    case "unavailable":
      callbacks.onConnectionState("unavailable", state.reason.message);
      break;
    case "session-expired":
      callbacks.onSessionExpired();
      break;
    case "stopped":
      break;
  }
}

/**
 * Bridges the transport-level SseClient callbacks into state projection. The
 * hook uses this bridge by default, while tests may inject a smaller stream.
 */
export function createSseAgentStreamFactory(
  api: AgentApiAdapter,
  options: Omit<SseClientOptions, "callbacks"> = {},
): (callbacks: AgentStreamCallbacks) => AgentStreamAdapter {
  const schemaVersion = options.schemaVersion ?? 1;
  return (callbacks) =>
    new SseClient({
      ...options,
      callbacks: {
        onMessage: (message) => {
          if (message.kind === "service.state") {
            callbacks.onServiceState(message.state, message.generation);
            return;
          }
          if (message.kind === "stream.reset") {
            callbacks.onConnectionState("recovering", "stream_reset");
            return;
          }
          callbacks.onTaskEvent(message);
        },
        onDiagnostic: (diagnostic) =>
          callbacks.onUnknownEvent(
            diagnostic.persistedId,
            diagnostic.event,
            schemaVersion,
          ),
        onState: (state) => projectSseState(state, callbacks),
        recover: async (_reason, signal) => {
          const bootstrap = await api.bootstrap(signal);
          if (signal.aborted) {
            throw new DOMException("bootstrap recovery aborted", "AbortError");
          }
          callbacks.onBootstrap(bootstrap);
          return bootstrap.latest_event_id;
        },
      },
    });
}

export function useAgentState(
  dependencies: UseAgentStateDependencies,
): UseAgentStateResult {
  const [state, dispatch] = useReducer(agentReducer, initialAgentState);
  const dependenciesRef = useRef(dependencies);
  const streamRef = useRef<AgentStreamAdapter | null>(null);
  const mountedRef = useRef(true);
  dependenciesRef.current = dependencies;

  const expireSession = useCallback(() => {
    const stream = streamRef.current;
    streamRef.current = null;
    try {
      stream?.stop();
    } catch {
      // Session expiry still wins if transport cleanup itself fails.
    }
    if (mountedRef.current) {
      dispatch({ type: "session.expired" });
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    let disposed = false;
    let stream: AgentStreamAdapter | null = null;
    dispatch({ type: "bootstrap.started" });

    const activeDependencies = dependenciesRef.current;
    const createStream =
      activeDependencies.createStream ??
      createSseAgentStreamFactory(activeDependencies.api);

    void activeDependencies.api
      .initialize()
      .then((bootstrap) => {
        if (disposed) {
          return;
        }
        dispatch({ type: "bootstrap.received", bootstrap });
        stream = createStream({
          onTaskEvent: (event) => dispatch({ type: "event.received", event }),
          onUnknownEvent: (id, kind, schemaVersion) =>
            dispatch({ type: "event.unknown", id, kind, schemaVersion }),
          onServiceState: (serviceState, generation) =>
            dispatch({
              type: "service.received",
              state: serviceState,
              generation,
            }),
          onBootstrap: (nextBootstrap) =>
            dispatch({ type: "bootstrap.received", bootstrap: nextBootstrap }),
          onConnectionState: (connection, reason) =>
            dispatch({
              type: "connection.changed",
              connection,
              reason: reason ?? null,
            }),
          onSessionExpired: expireSession,
        });
        streamRef.current = stream;
        try {
          const started = stream.start(bootstrap.latest_event_id);
          void Promise.resolve(started).catch((error: unknown) => {
            if (!disposed) {
              if (isSessionExpired(error)) {
                expireSession();
              } else {
                dispatch({
                  type: "connection.changed",
                  connection: "unavailable",
                  reason: errorMessage(error),
                });
              }
            }
          });
        } catch (error) {
          if (isSessionExpired(error)) {
            expireSession();
          } else {
            dispatch({
              type: "connection.changed",
              connection: "unavailable",
              reason: errorMessage(error),
            });
          }
        }
      })
      .catch((error: unknown) => {
        if (!disposed) {
          if (isSessionExpired(error)) {
            expireSession();
          } else {
            dispatch({
              type: "bootstrap.failed",
              reason: errorMessage(error),
              protocol: false,
            });
          }
        }
      });

    return () => {
      disposed = true;
      mountedRef.current = false;
      if (streamRef.current === stream) {
        streamRef.current = null;
        stream?.stop();
      }
    };
  }, [expireSession]);

  useEffect(() => {
    if (state.selectedTaskId === null || !state.detailLoading) {
      return;
    }
    const taskId = state.selectedTaskId;
    const generation = state.selectionGeneration;
    let disposed = false;
    void dependenciesRef.current.api
      .taskDetail(taskId)
      .then((detail) => {
        if (!disposed) {
          dispatch({ type: "detail.received", taskId, generation, detail });
        }
      })
      .catch((error: unknown) => {
        if (isSessionExpired(error) && mountedRef.current) {
          expireSession();
        } else if (!disposed) {
          dispatch({
            type: "detail.failed",
            taskId,
            generation,
            message: errorMessage(error),
          });
        }
      });
    return () => {
      disposed = true;
    };
  }, [
    state.detailLoading,
    state.selectedTaskId,
    state.selectionGeneration,
  ]);

  const selectTask = useCallback((taskId: string) => {
    dispatch({ type: "task.selected", taskId });
  }, []);

  const addRepository = useCallback(
    async (path: string): Promise<Repository> => {
      try {
        const repository = await dependenciesRef.current.api.addRepository(path);
        dispatch({ type: "repository.upserted", repository });
        return repository;
      } catch (error) {
        if (isSessionExpired(error)) {
          expireSession();
        }
        throw error;
      }
    },
    [expireSession],
  );

  const pickRepository = useCallback(async (): Promise<Repository | null> => {
    try {
      const repository = await dependenciesRef.current.api.pickRepository();
      if (repository !== null) {
        dispatch({ type: "repository.upserted", repository });
      }
      return repository;
    } catch (error) {
      if (isSessionExpired(error)) {
        expireSession();
      }
      throw error;
    }
  }, [expireSession]);

  const newCreateTask = useCallback(
    (repositoryId: string, prompt: string): CreateTaskCommand => {
      let command: CreateTaskCommand;
      try {
        command = dependenciesRef.current.api.newCreateTask(repositoryId, prompt);
      } catch (error) {
        if (isSessionExpired(error)) {
          expireSession();
        }
        throw error;
      }
      return {
        clientRequestId: command.clientRequestId,
        execute: async () => {
          try {
            const task = await command.execute();
            dispatch({ type: "task.upserted", task });
            return task;
          } catch (error) {
            if (isSessionExpired(error)) {
              expireSession();
            }
            throw error;
          }
        },
      };
    },
    [expireSession],
  );

  const cancelTask = useCallback(
    async (taskId: string): Promise<Task> => {
      dispatch({ type: "cancel.started", taskId });
      try {
        const response = await dependenciesRef.current.api.cancelTask(taskId);
        dispatch({ type: "cancel.succeeded", response });
        return response.task;
      } catch (error) {
        if (isSessionExpired(error)) {
          expireSession();
        } else {
          dispatch({ type: "cancel.failed", taskId, error: commandError(error) });
        }
        throw error;
      }
    },
    [expireSession],
  );

  const retryTask = useCallback(
    async (taskId: string): Promise<Task> => {
      try {
        const task = await dependenciesRef.current.api.retryTask(taskId);
        dispatch({ type: "task.upserted", task });
        return task;
      } catch (error) {
        if (isSessionExpired(error)) {
          expireSession();
        }
        throw error;
      }
    },
    [expireSession],
  );

  const quit = useCallback(async (): Promise<QuitResponse> => {
    try {
      return await dependenciesRef.current.api.quit();
    } catch (error) {
      if (isSessionExpired(error)) {
        expireSession();
      }
      throw error;
    }
  }, [expireSession]);

  return {
    state,
    selectTask,
    addRepository,
    pickRepository,
    newCreateTask,
    cancelTask,
    retryTask,
    quit,
  };
}
