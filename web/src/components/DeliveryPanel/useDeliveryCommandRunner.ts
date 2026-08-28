import { useCallback, useEffect, useRef, useState } from "react";

import type { DeliveryCommand } from "../../api/deliveryClient";
import type { DeliveryCommandResponse } from "../../api/types";
import type { DeliveryErrorState } from "../../state/deliveryModel";

export type DeliveryCommandExecutionState =
  | { phase: "idle" }
  | { phase: "pending"; clientRequestId: string; scopeKey: string }
  | {
      phase: "error";
      clientRequestId: string;
      scopeKey: string;
      error: DeliveryErrorState;
    }
  | {
      phase: "succeeded";
      clientRequestId: string;
      scopeKey: string;
      response: DeliveryCommandResponse;
    };

export interface DeliveryCommandRunner {
  state: DeliveryCommandExecutionState;
  start(
    scopeKey: string,
    command: DeliveryCommand,
    handlers: DeliveryCommandSettlementHandlers,
  ): void;
  retry(): void;
  reset(): void;
}

export interface DeliveryCommandSettlementHandlers {
  onSuccess(response: DeliveryCommandResponse): void;
  onFailure(error: DeliveryErrorState): void;
}

export function useDeliveryCommandRunner(
  taskId: string,
): DeliveryCommandRunner {
  const [state, setState] = useState<DeliveryCommandExecutionState>({
    phase: "idle",
  });
  const stateRef = useRef(state);
  const commandRef = useRef<DeliveryCommand | null>(null);
  const handlersRef = useRef<DeliveryCommandSettlementHandlers | null>(null);
  const requestRef = useRef<AbortController | null>(null);
  const epochRef = useRef(0);
  const renderedTaskRef = useRef(taskId);
  const runnerTaskRef = useRef(taskId);
  renderedTaskRef.current = taskId;

  const commit = useCallback((next: DeliveryCommandExecutionState) => {
    stateRef.current = next;
    setState(next);
  }, []);

  const invalidate = useCallback(() => {
    epochRef.current += 1;
    requestRef.current?.abort();
    requestRef.current = null;
  }, []);

  const reset = useCallback(() => {
    invalidate();
    commandRef.current = null;
    handlersRef.current = null;
    commit({ phase: "idle" });
  }, [commit, invalidate]);

  const execute = useCallback(
    (scopeOverride?: string) => {
      const command = commandRef.current;
      const current = stateRef.current;
      if (command === null || current.phase === "pending") return;
      const scopeKey =
        scopeOverride ?? (current.phase === "idle" ? null : current.scopeKey);
      if (scopeKey === null) return;
      invalidate();
      const request = new AbortController();
      requestRef.current = request;
      const epoch = epochRef.current;
      const commandTaskId = renderedTaskRef.current;
      commit({
        phase: "pending",
        clientRequestId: command.clientRequestId,
        scopeKey,
      });
      void (async () => {
        try {
          const response = await command.execute(request.signal);
          if (
            request.signal.aborted ||
            requestRef.current !== request ||
            epochRef.current !== epoch ||
            renderedTaskRef.current !== commandTaskId
          ) {
            return;
          }
          requestRef.current = null;
          commit({
            phase: "succeeded",
            clientRequestId: command.clientRequestId,
            scopeKey,
            response,
          });
          handlersRef.current?.onSuccess(response);
        } catch (error) {
          if (
            request.signal.aborted ||
            requestRef.current !== request ||
            epochRef.current !== epoch ||
            renderedTaskRef.current !== commandTaskId
          ) {
            return;
          }
          requestRef.current = null;
          const failure = commandFailure(error);
          commit({
            phase: "error",
            clientRequestId: command.clientRequestId,
            scopeKey,
            error: failure,
          });
          handlersRef.current?.onFailure(failure);
        }
      })();
    },
    [commit, invalidate],
  );

  const start = useCallback(
    (
      scopeKey: string,
      command: DeliveryCommand,
      handlers: DeliveryCommandSettlementHandlers,
    ) => {
      if (stateRef.current.phase === "pending") return;
      invalidate();
      commandRef.current = command;
      handlersRef.current = handlers;
      execute(scopeKey);
    },
    [execute, invalidate],
  );

  useEffect(() => {
    if (runnerTaskRef.current !== taskId) {
      invalidate();
      commandRef.current = null;
      handlersRef.current = null;
      runnerTaskRef.current = taskId;
      commit({ phase: "idle" });
    }
    return () => {
      invalidate();
      commandRef.current = null;
      handlersRef.current = null;
    };
  }, [commit, invalidate, taskId]);

  return { state, start, retry: execute, reset };
}

export function prepareDeliveryCommandRunner(
  runner: DeliveryCommandRunner,
  scopeKey: string,
): void {
  if (
    runner.state.phase === "error" &&
    runner.state.scopeKey === scopeKey &&
    runner.state.error.retryable
  ) {
    return;
  }
  runner.reset();
}

function commandFailure(value: unknown): DeliveryErrorState {
  if (typeof value === "object" && value !== null) {
    const candidate = value as Record<string, unknown>;
    return {
      code:
        typeof candidate.code === "string"
          ? candidate.code
          : "DELIVERY_REQUEST_FAILED",
      message:
        typeof candidate.message === "string"
          ? candidate.message
          : "The delivery request failed.",
      retryable: candidate.retryable !== false,
      requestId:
        typeof candidate.requestId === "string" ? candidate.requestId : null,
    };
  }
  return {
    code: "DELIVERY_REQUEST_FAILED",
    message: "The delivery request failed.",
    retryable: true,
    requestId: null,
  };
}
