import type { DeliveryOperation, DeliveryTask } from "../api/types";
import {
  DELIVERY_INITIAL_POLL_DELAY_MS,
  DELIVERY_MAX_POLL_DELAY_MS,
  latestOperationFromTask,
  shouldPollDeliveryOperation,
  type DeliveryErrorState,
  type DeliveryModalState,
  type DeliveryState,
} from "./deliveryModel";

export type DeliveryAction =
  | { type: "task.selected"; taskId: string | null; generation: number }
  | { type: "delivery.started"; taskId: string; generation: number }
  | {
      type: "delivery.received";
      taskId: string;
      generation: number;
      projection: DeliveryTask;
    }
  | {
      type: "delivery.failed";
      taskId: string;
      generation: number;
      error: DeliveryErrorState;
    }
  | {
      type: "operation.tracked";
      taskId: string;
      generation: number;
      operation: DeliveryOperation;
    }
  | {
      type: "operation.received";
      taskId: string;
      generation: number;
      operation: DeliveryOperation;
    }
  | {
      type: "operation.failed";
      taskId: string;
      generation: number;
      operationId: string;
      error: DeliveryErrorState;
      nextDelayMs: number;
      willRetry: boolean;
    }
  | { type: "modal.opened"; modal: DeliveryModalState }
  | { type: "modal.cleared" };

export function deliveryReducer(
  state: DeliveryState,
  action: DeliveryAction,
): DeliveryState {
  switch (action.type) {
    case "task.selected":
      return {
        taskId: action.taskId,
        generation: action.generation,
        phase: action.taskId === null ? "idle" : "loading",
        projection: null,
        operation: null,
        trackedOperationId: null,
        pollDelayMs: DELIVERY_INITIAL_POLL_DELAY_MS,
        error: null,
        modal: null,
      };
    case "delivery.started":
      if (!matchesTask(state, action.taskId, action.generation)) return state;
      return { ...state, phase: "loading", error: null };
    case "delivery.received": {
      if (!matchesTask(state, action.taskId, action.generation)) return state;
      if (action.projection.task_id !== action.taskId) return state;
      const latest = latestOperationFromTask(action.projection);
      const operation = chooseNewestOperation(state.operation, latest);
      const active = operation !== null && shouldPollDeliveryOperation(operation);
      return {
        ...state,
        phase: active ? "polling" : "ready",
        projection: action.projection,
        operation,
        trackedOperationId: active ? operation.operation_id : null,
        pollDelayMs: DELIVERY_INITIAL_POLL_DELAY_MS,
        error: null,
        modal: modalStillFresh(
          state.modal,
          action.taskId,
          operation,
          action.projection,
        ),
      };
    }
    case "delivery.failed":
      if (!matchesTask(state, action.taskId, action.generation)) return state;
      return { ...state, phase: "error", error: action.error };
    case "operation.tracked": {
      if (!matchesTask(state, action.taskId, action.generation)) return state;
      if (
        state.operation !== null &&
        state.operation.operation_id === action.operation.operation_id &&
        state.operation.version > action.operation.version
      ) {
        return state;
      }
      const polling = shouldPollDeliveryOperation(action.operation);
      return {
        ...state,
        phase: polling ? "polling" : "refreshing",
        operation: action.operation,
        trackedOperationId: polling ? action.operation.operation_id : null,
        pollDelayMs: DELIVERY_INITIAL_POLL_DELAY_MS,
        error: null,
        modal: modalStillFresh(
          state.modal,
          action.taskId,
          action.operation,
          state.projection,
        ),
      };
    }
    case "operation.received": {
      if (!matchesTask(state, action.taskId, action.generation)) return state;
      if (state.trackedOperationId !== action.operation.operation_id) return state;
      const current = state.operation;
      if (
        current !== null &&
        current.operation_id === action.operation.operation_id &&
        action.operation.version < current.version
      ) {
        return state;
      }
      const progressed =
        current === null ||
        current.operation_id !== action.operation.operation_id ||
        action.operation.version > current.version;
      const polling = shouldPollDeliveryOperation(action.operation);
      return {
        ...state,
        phase: polling ? "polling" : "refreshing",
        operation: action.operation,
        trackedOperationId: polling ? action.operation.operation_id : null,
        pollDelayMs: progressed
          ? DELIVERY_INITIAL_POLL_DELAY_MS
          : Math.min(state.pollDelayMs * 2, DELIVERY_MAX_POLL_DELAY_MS),
        error: null,
        modal: modalStillFresh(
          state.modal,
          action.taskId,
          action.operation,
          state.projection,
        ),
      };
    }
    case "operation.failed":
      if (
        !matchesTask(state, action.taskId, action.generation) ||
        state.trackedOperationId !== action.operationId
      ) {
        return state;
      }
      return {
        ...state,
        phase: action.willRetry ? "polling" : "error",
        pollDelayMs: action.nextDelayMs,
        error: action.error,
      };
    case "modal.opened":
      if (action.modal.taskId !== state.taskId) return state;
      return { ...state, modal: action.modal };
    case "modal.cleared":
      return state.modal === null ? state : { ...state, modal: null };
  }
}

function matchesTask(
  state: DeliveryState,
  taskId: string,
  generation: number,
): boolean {
  return state.taskId === taskId && state.generation === generation;
}

function chooseNewestOperation(
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

function modalStillFresh(
  modal: DeliveryModalState | null,
  taskId: string,
  operation: DeliveryOperation | null,
  projection: DeliveryTask | null,
): DeliveryModalState | null {
  if (modal === null || modal.taskId !== taskId) return null;
  if (modal.authority !== null) {
    const evidence = projection?.evidence;
    const target = projection?.target;
    if (
      evidence == null ||
      target == null ||
      !("branch" in target) ||
      target.available !== true ||
      evidence.review_generation !== modal.authority.reviewGeneration ||
      evidence.workspace_fingerprint !== modal.authority.workspaceFingerprint ||
      target.branch !== modal.authority.targetBranch ||
      target.head !== modal.authority.targetHead
    ) {
      return null;
    }
  }
  if (modal.operationId === null) return modal;
  if (
    operation === null ||
    operation.operation_id !== modal.operationId ||
    operation.version !== modal.operationVersion
  ) {
    return null;
  }
  return modal;
}
