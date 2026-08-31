import {
  useCallback,
  useEffect,
  useReducer,
  useRef,
  type Dispatch,
} from "react";

import type { DeliveryOperation, DeliveryTask } from "../api/types";
import type { DeliveryClient } from "../api/deliveryClient";
import { deliveryReducer } from "./deliveryReducer";
import {
  DELIVERY_INITIAL_POLL_DELAY_MS,
  DELIVERY_MAX_POLL_DELAY_MS,
  initialDeliveryState,
  latestOperationFromTask,
  shouldPollDeliveryOperation,
  type DeliveryErrorState,
  type DeliveryModalState,
  type DeliveryState,
} from "./deliveryModel";

export interface DeliveryPollingApi {
  taskDelivery(taskId: string, signal?: AbortSignal): Promise<DeliveryTask>;
  deliveryOperation(
    operationId: string,
    signal?: AbortSignal,
  ): Promise<DeliveryOperation>;
}

export interface DeliveryPollingClock {
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

export interface UseDeliveryPollingOptions {
  api: DeliveryPollingApi | DeliveryClient;
  taskId: string | null;
  clock?: DeliveryPollingClock;
}

export interface DeliveryPollingController {
  state: DeliveryState;
  refresh(): void;
  trackOperation(operation: DeliveryOperation): void;
  openModal(modal: Omit<DeliveryModalState, "taskId">): void;
  clearModal(): void;
}

interface ActivePollingSession {
  refresh(taskId: string | null): void;
  track(taskId: string, operation: DeliveryOperation): void;
  dispose(): void;
}

const browserClock: DeliveryPollingClock = {
  setTimeout: (callback, delayMs) => globalThis.setTimeout(callback, delayMs),
  clearTimeout: (handle) => globalThis.clearTimeout(handle as number),
};

export function useDeliveryPolling({
  api,
  taskId,
  clock = browserClock,
}: UseDeliveryPollingOptions): DeliveryPollingController {
  const [state, dispatch] = useReducer(deliveryReducer, initialDeliveryState);
  const sessionRef = useRef<ActivePollingSession | null>(null);
  const generationRef = useRef(0);

  useEffect(() => {
    generationRef.current += 1;
    const generation = generationRef.current;
    dispatch({ type: "task.selected", taskId, generation });
    const session = createPollingSession({
      api,
      taskId,
      generation,
      clock,
      dispatch,
    });
    sessionRef.current = session;
    session.refresh(taskId);
    return () => {
      if (sessionRef.current === session) sessionRef.current = null;
      session.dispose();
    };
  }, [api, clock, taskId]);

  const refresh = useCallback(
    () => sessionRef.current?.refresh(taskId),
    [taskId],
  );
  const trackOperation = useCallback(
    (operation: DeliveryOperation) => {
      if (taskId === null) return;
      sessionRef.current?.track(taskId, operation);
    },
    [taskId],
  );
  const openModal = useCallback(
    (modal: Omit<DeliveryModalState, "taskId">) => {
      if (taskId === null) return;
      dispatch({ type: "modal.opened", modal: { ...modal, taskId } });
    },
    [taskId],
  );
  const clearModal = useCallback(() => dispatch({ type: "modal.cleared" }), []);

  return { state, refresh, trackOperation, openModal, clearModal };
}

function createPollingSession({
  api,
  taskId,
  generation,
  clock,
  dispatch,
}: {
  api: DeliveryPollingApi;
  taskId: string | null;
  generation: number;
  clock: DeliveryPollingClock;
  dispatch: Dispatch<Parameters<typeof deliveryReducer>[1]>;
}): ActivePollingSession {
  let disposed = false;
  let controller: AbortController | null = null;
  let timer: unknown = null;
  let tracked: DeliveryOperation | null = null;
  let lastSeen: DeliveryOperation | null = null;
  let nextDelayMs = DELIVERY_INITIAL_POLL_DELAY_MS;
  // An aborted HTTP request can leave its backend query running, so keep the
  // active same-task request and coalesce freshness demand into one trailing load.
  let trailingRefreshRequested = false;

  const clearTimer = () => {
    if (timer !== null) {
      clock.clearTimeout(timer);
      timer = null;
    }
  };
  const abortRequest = () => {
    controller?.abort();
    controller = null;
  };
  const active = (request: AbortController) =>
    !disposed && controller === request && !request.signal.aborted;

  const schedule = (operationId: string, delayMs: number) => {
    clearTimer();
    if (disposed || taskId === null) return;
    timer = clock.setTimeout(() => {
      timer = null;
      void poll(operationId);
    }, delayMs);
  };

  const runTrailingRefresh = (): boolean => {
    if (
      !trailingRefreshRequested ||
      disposed ||
      taskId === null ||
      controller !== null ||
      timer !== null
    ) {
      return false;
    }
    trailingRefreshRequested = false;
    void loadDelivery();
    return true;
  };

  const loadDelivery = async () => {
    if (disposed || taskId === null) return;
    if (controller !== null || timer !== null) {
      trailingRefreshRequested = true;
      return;
    }
    trailingRefreshRequested = false;
    clearTimer();
    const request = new AbortController();
    controller = request;
    dispatch({ type: "delivery.started", taskId, generation });
    try {
      const projection = await api.taskDelivery(taskId, request.signal);
      if (!active(request)) return;
      controller = null;
      dispatch({ type: "delivery.received", taskId, generation, projection });
      const projected = latestOperationFromTask(projection);
      const latest = newestOperation(lastSeen, projected);
      lastSeen = latest;
      if (latest !== null && shouldPollDeliveryOperation(latest)) {
        tracked = latest;
        nextDelayMs = DELIVERY_INITIAL_POLL_DELAY_MS;
        if (!runTrailingRefresh()) schedule(latest.operation_id, nextDelayMs);
      } else {
        tracked = null;
        runTrailingRefresh();
      }
    } catch (error) {
      if (!active(request)) return;
      controller = null;
      dispatch({
        type: "delivery.failed",
        taskId,
        generation,
        error: deliveryError(error),
      });
      runTrailingRefresh();
    }
  };

  const poll = async (operationId: string) => {
    if (
      disposed ||
      taskId === null ||
      tracked === null ||
      tracked.operation_id !== operationId
    ) {
      return;
    }
    abortRequest();
    const request = new AbortController();
    controller = request;
    try {
      const operation = await api.deliveryOperation(operationId, request.signal);
      if (!active(request) || tracked?.operation_id !== operationId) return;
      controller = null;
      if (operation.operation_id !== operationId) {
        runTrailingRefresh();
        return;
      }
      if (
        lastSeen !== null &&
        lastSeen.operation_id === operationId &&
        operation.version < lastSeen.version
      ) {
        if (!runTrailingRefresh()) schedule(operationId, nextDelayMs);
        return;
      }
      const progressed =
        lastSeen === null ||
        lastSeen.operation_id !== operationId ||
        operation.version > lastSeen.version;
      lastSeen = operation;
      tracked = operation;
      nextDelayMs = progressed
        ? DELIVERY_INITIAL_POLL_DELAY_MS
        : Math.min(nextDelayMs * 2, DELIVERY_MAX_POLL_DELAY_MS);
      dispatch({
        type: "operation.received",
        taskId,
        generation,
        operation,
      });
      if (shouldPollDeliveryOperation(operation)) {
        if (!runTrailingRefresh()) schedule(operationId, nextDelayMs);
      } else {
        tracked = null;
        void loadDelivery();
      }
    } catch (error) {
      if (!active(request) || tracked?.operation_id !== operationId) return;
      controller = null;
      const projected = deliveryError(error);
      const willRetry = projected.retryable;
      nextDelayMs = Math.min(nextDelayMs * 2, DELIVERY_MAX_POLL_DELAY_MS);
      dispatch({
        type: "operation.failed",
        taskId,
        generation,
        operationId,
        error: projected,
        nextDelayMs,
        willRetry,
      });
      if (willRetry) {
        if (!runTrailingRefresh()) schedule(operationId, nextDelayMs);
      } else {
        runTrailingRefresh();
      }
    }
  };

  return {
    refresh(expectedTaskId) {
      if (expectedTaskId !== taskId) return;
      if (controller !== null || timer !== null) {
        trailingRefreshRequested = true;
        return;
      }
      void loadDelivery();
    },
    track(expectedTaskId, operation) {
      if (disposed || taskId === null || expectedTaskId !== taskId) return;
      if (
        lastSeen !== null &&
        lastSeen.operation_id === operation.operation_id &&
        operation.version < lastSeen.version
      ) {
        return;
      }
      clearTimer();
      abortRequest();
      trailingRefreshRequested = false;
      lastSeen = operation;
      tracked = operation;
      nextDelayMs = DELIVERY_INITIAL_POLL_DELAY_MS;
      dispatch({
        type: "operation.tracked",
        taskId,
        generation,
        operation,
      });
      if (shouldPollDeliveryOperation(operation)) {
        schedule(operation.operation_id, nextDelayMs);
      } else {
        tracked = null;
        void loadDelivery();
      }
    },
    dispose() {
      disposed = true;
      clearTimer();
      abortRequest();
      trailingRefreshRequested = false;
      tracked = null;
      lastSeen = null;
    },
  };
}

function newestOperation(
  current: DeliveryOperation | null,
  incoming: DeliveryOperation | null,
): DeliveryOperation | null {
  if (incoming === null) return current;
  if (
    current !== null &&
    current.operation_id !== incoming.operation_id &&
    shouldPollDeliveryOperation(current)
  ) {
    return current;
  }
  if (
    current !== null &&
    current.operation_id === incoming.operation_id &&
    current.version > incoming.version
  ) {
    return current;
  }
  return incoming;
}

function deliveryError(value: unknown): DeliveryErrorState {
  if (typeof value === "object" && value !== null) {
    const candidate = value as Record<string, unknown>;
    return {
      code: typeof candidate.code === "string" ? candidate.code : "DELIVERY_REQUEST_FAILED",
      message:
        typeof candidate.message === "string"
          ? candidate.message
          : "the delivery request failed",
      retryable: candidate.retryable === true,
      requestId:
        typeof candidate.requestId === "string" ? candidate.requestId : null,
    };
  }
  return {
    code: "DELIVERY_REQUEST_FAILED",
    message: "the delivery request failed",
    retryable: false,
    requestId: null,
  };
}
