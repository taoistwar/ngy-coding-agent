import type {
  BootstrapResponse,
  CancellationAcceptedResponse,
  Repository,
  Task,
  TaskDetail,
  TaskEvent,
  TimelineEntry,
} from "../api/types";
import type {
  AgentConnectionState,
  AgentState,
  CommandError,
  IgnoredEventDiagnostic,
} from "./model";
import {
  projectReviewEvent,
  type ReviewProjectionResult,
} from "./reviewProjection";

export type AgentAction =
  | { type: "bootstrap.started" }
  | { type: "bootstrap.received"; bootstrap: BootstrapResponse }
  | { type: "bootstrap.failed"; reason: string; protocol: boolean }
  | { type: "connection.changed"; connection: AgentConnectionState; reason: string | null }
  | { type: "session.expired" }
  | { type: "repository.upserted"; repository: Repository }
  | { type: "task.upserted"; task: Task }
  | { type: "task.selected"; taskId: string }
  | {
      type: "detail.received";
      taskId: string;
      generation: number;
      detail: TaskDetail;
    }
  | { type: "detail.failed"; taskId: string; generation: number; message: string }
  | { type: "event.received"; event: TaskEvent }
  | {
      type: "event.conflicted";
      event: Extract<TaskEvent, { kind: "review.updated" }>;
      reason: Extract<
        ReviewProjectionResult,
        { kind: "conflict" }
      >["reason"];
    }
  | {
      type: "recovery.received";
      bootstrap: BootstrapResponse;
      bufferedEvents: TaskEvent[];
    }
  | { type: "event.unknown"; id: number; kind: string; schemaVersion: number }
  | {
      type: "service.received";
      state: BootstrapResponse["service_state"];
      generation: number;
    }
  | { type: "cancel.started"; taskId: string }
  | { type: "cancel.succeeded"; response: CancellationAcceptedResponse }
  | { type: "cancel.failed"; taskId: string; error: CommandError };

interface Normalized<T> {
  byId: Record<string, T>;
  order: string[];
}

const SUPPORTED_SCHEMA_VERSION = 1;
const MAX_DIAGNOSTICS = 100;

type LifecycleKind =
  | "task.queued"
  | "task.started"
  | "task.completed"
  | "task.failed"
  | "task.cancelled"
  | "task.interrupted";
type LifecycleEvent = Extract<TaskEvent, { kind: LifecycleKind }>;

function normalizeById<T extends { id: string }>(values: T[]): Normalized<T> {
  const byId: Record<string, T> = {};
  const order: string[] = [];
  for (const value of values) {
    if (!(value.id in byId)) {
      order.push(value.id);
    }
    byId[value.id] = value;
  }
  return { byId, order };
}

function protocolError(state: AgentState, reason: string): AgentState {
  return {
    ...state,
    connection: "protocol_error",
    recoveryReason: reason,
  };
}

function withTask(state: AgentState, value: Task): Pick<AgentState, "tasksById" | "taskOrder"> {
  const exists = value.id in state.tasksById;
  return {
    tasksById: { ...state.tasksById, [value.id]: value },
    taskOrder: exists ? state.taskOrder : [...state.taskOrder, value.id],
  };
}

function withRepository(
  state: AgentState,
  value: Repository,
): Pick<AgentState, "repositoriesById" | "repositoryOrder"> {
  const exists = value.id in state.repositoriesById;
  return {
    repositoriesById: { ...state.repositoriesById, [value.id]: value },
    repositoryOrder: exists
      ? state.repositoryOrder
      : [...state.repositoryOrder, value.id],
  };
}

function isLifecycleEvent(event: TaskEvent): event is LifecycleEvent {
  return (
    event.kind === "task.queued" ||
    event.kind === "task.started" ||
    event.kind === "task.completed" ||
    event.kind === "task.failed" ||
    event.kind === "task.cancelled" ||
    event.kind === "task.interrupted"
  );
}

function isTerminalTask(task: Task): boolean {
  return (
    task.status === "completed" ||
    task.status === "failed" ||
    task.status === "cancelled" ||
    task.status === "interrupted"
  );
}

function reconcileCommands(
  state: AgentState,
  tasksById: Record<string, Task>,
): AgentState["commands"] {
  const cancelByTaskId: AgentState["commands"]["cancelByTaskId"] = {};
  for (const [taskId, command] of Object.entries(state.commands.cancelByTaskId)) {
    const task = tasksById[taskId];
    if (task !== undefined && !isTerminalTask(task)) {
      cancelByTaskId[taskId] = command;
    }
  }
  return { ...state.commands, cancelByTaskId };
}

function upsertRestTask(state: AgentState, task: Task): AgentState {
  const current = state.tasksById[task.id];
  if (
    current !== undefined &&
    (task.last_event_id < current.last_event_id ||
      (isTerminalTask(current) && !isTerminalTask(task)))
  ) {
    return state;
  }
  return { ...state, ...withTask(state, task) };
}

function lifecycleLabel(event: LifecycleEvent): string {
  switch (event.kind) {
    case "task.queued":
      return "Task queued";
    case "task.started":
      return "Task started";
    case "task.completed":
      return "Task completed";
    case "task.failed":
      return "Task failed";
    case "task.cancelled":
      return "Task cancelled";
    case "task.interrupted":
      return "Task interrupted";
  }
}

function timelineEntry(event: LifecycleEvent): TimelineEntry {
  const task = event.payload.task;
  const base: TimelineEntry = {
    event_id: event.id,
    kind: event.kind,
    label: lifecycleLabel(event),
    created_at: event.created_at,
  };
  return task.failure == null ? base : { ...base, failure: task.failure };
}

function updateDetailTaskCursor(detail: TaskDetail, event: TaskEvent): TaskDetail {
  if (detail.task.last_event_id >= event.id) {
    return detail;
  }
  return {
    ...detail,
    task: { ...detail.task, last_event_id: event.id },
  };
}

type DetailProjection =
  | { kind: "applied"; detail: TaskDetail }
  | Extract<ReviewProjectionResult, { kind: "stale" | "conflict" }>;

function applyEventToDetail(
  detail: TaskDetail,
  event: TaskEvent,
): DetailProjection {
  if (event.task_id !== detail.task.id || event.id <= detail.event_cursor) {
    return { kind: "applied", detail };
  }

  if (event.kind === "review.updated") {
    const reviewProjection = projectReviewEvent(detail, event);
    if (reviewProjection.kind === "conflict" || reviewProjection.kind === "stale") {
      return reviewProjection;
    }
    return { kind: "applied", detail: reviewProjection.detail };
  }

  let next = updateDetailTaskCursor(detail, event);
  switch (event.kind) {
    case "plan.updated":
      next = { ...next, plan: event.payload.plan };
      break;
    case "activity.appended":
      if (!next.activity.some((entry) => entry.id === event.payload.entry.id)) {
        next = { ...next, activity: [...next.activity, event.payload.entry] };
      }
      break;
    case "diff.updated":
      next = { ...next, diff: event.payload.diff };
      break;
    case "test.updated":
      next = { ...next, tests: event.payload.tests };
      break;
    case "task.queued":
    case "task.started":
    case "task.completed":
    case "task.failed":
    case "task.cancelled":
    case "task.interrupted": {
      const entry = timelineEntry(event);
      const timeline =
        next.timeline.some((item) => item.event_id === event.id)
          ? next.timeline
          : [...next.timeline, entry];
      next = { ...next, task: event.payload.task, timeline };
      break;
    }
  }

  return { kind: "applied", detail: { ...next, event_cursor: event.id } };
}

function updateSummaryForEvent(
  state: AgentState,
  event: TaskEvent,
): Pick<AgentState, "tasksById" | "taskOrder"> {
  if (isLifecycleEvent(event)) {
    const current = state.tasksById[event.task_id];
    if (
      current !== undefined &&
      (event.payload.task.last_event_id <= current.last_event_id ||
        (isTerminalTask(current) && !isTerminalTask(event.payload.task)))
    ) {
      return { tasksById: state.tasksById, taskOrder: state.taskOrder };
    }
    return withTask(state, event.payload.task);
  }

  const current = state.tasksById[event.task_id];
  if (current === undefined || current.last_event_id >= event.id) {
    return { tasksById: state.tasksById, taskOrder: state.taskOrder };
  }
  return {
    tasksById: {
      ...state.tasksById,
      [event.task_id]: { ...current, last_event_id: event.id },
    },
    taskOrder: state.taskOrder,
  };
}

function removeCancelCommand(state: AgentState, taskId: string): AgentState["commands"] {
  if (!(taskId in state.commands.cancelByTaskId)) {
    return state.commands;
  }
  const cancelByTaskId = { ...state.commands.cancelByTaskId };
  delete cancelByTaskId[taskId];
  return { ...state.commands, cancelByTaskId };
}

function appendUniqueEvent(events: TaskEvent[], event: TaskEvent): TaskEvent[] {
  return events.some((candidate) => candidate.id === event.id)
    ? events
    : [...events, event];
}

function beginSnapshotRecovery(
  state: AgentState,
  event: Extract<TaskEvent, { kind: "review.updated" }>,
  reason: Extract<ReviewProjectionResult, { kind: "conflict" }>["reason"],
): AgentState {
  return {
    ...state,
    connection: "recovering",
    recoveryReason: reason,
    snapshotRecovery:
      state.snapshotRecovery ?? {
        conflictEventId: event.id,
        reason,
      },
    recoveryBuffer: appendUniqueEvent(state.recoveryBuffer, event),
    diagnostics: appendDiagnostic(state, {
      id: event.id,
      kind: event.kind,
      schemaVersion: event.schema_version,
      message:
        "Conflicting review evidence triggered an authoritative snapshot recovery.",
    }),
  };
}

export function inspectEventProjection(
  state: AgentState,
  event: TaskEvent,
): Extract<ReviewProjectionResult, { kind: "conflict" }> | null {
  if (
    event.kind !== "review.updated" ||
    state.snapshotRecovery !== null ||
    state.detailStale ||
    state.selectedTaskId !== event.task_id ||
    state.selectedDetail === null ||
    state.selectedDetail.task.id !== event.task_id
  ) {
    return null;
  }
  const projection = projectReviewEvent(state.selectedDetail, event);
  return projection.kind === "conflict" ? projection : null;
}

function reducePersistedEvent(state: AgentState, event: TaskEvent): AgentState {
  if (event.schema_version !== SUPPORTED_SCHEMA_VERSION) {
    return protocolError(state, "unsupported_schema_version");
  }
  if (event.id === state.appliedEventId) {
    return state;
  }
  if (event.id < state.appliedEventId) {
    return protocolError(state, "non_monotonic_event_id");
  }

  if (state.snapshotRecovery !== null) {
    return {
      ...state,
      recoveryBuffer: appendUniqueEvent(state.recoveryBuffer, event),
    };
  }

  let detailProjection: DetailProjection | null = null;
  if (
    state.selectedTaskId === event.task_id &&
    state.selectedDetail !== null &&
    state.selectedDetail.task.id === event.task_id &&
    !state.detailStale
  ) {
    detailProjection = applyEventToDetail(state.selectedDetail, event);
    if (detailProjection.kind === "conflict") {
      if (event.kind !== "review.updated") {
        return protocolError(state, "detail_projection_conflict");
      }
      return beginSnapshotRecovery(state, event, detailProjection.reason);
    }
  }

  const summary = updateSummaryForEvent(state, event);
  let selectedDetail = state.selectedDetail;
  let liveBufferByTaskId = state.liveBufferByTaskId;
  let detailStale = state.detailStale;
  let detailLoading = state.detailLoading;
  let detailError = state.detailError;
  if (state.selectedTaskId === event.task_id) {
    if (
      selectedDetail !== null &&
      selectedDetail.task.id === event.task_id &&
      state.detailStale
    ) {
      const buffered = liveBufferByTaskId[event.task_id] ?? [];
      liveBufferByTaskId = {
        ...liveBufferByTaskId,
        [event.task_id]: appendUniqueEvent(buffered, event),
      };
      const withCursor = updateDetailTaskCursor(selectedDetail, event);
      selectedDetail = {
        ...withCursor,
        event_cursor: Math.max(withCursor.event_cursor, event.id),
      };
      if (!detailLoading) {
        detailLoading = true;
        detailError = null;
      }
    } else if (detailProjection?.kind === "stale") {
      selectedDetail = detailProjection.detail;
      detailStale = true;
      detailLoading = true;
      detailError = null;
      const buffered = liveBufferByTaskId[event.task_id] ?? [];
      liveBufferByTaskId = {
        ...liveBufferByTaskId,
        [event.task_id]: appendUniqueEvent(buffered, event),
      };
    } else if (detailProjection?.kind === "applied") {
      selectedDetail = detailProjection.detail;
    } else {
      const buffered = liveBufferByTaskId[event.task_id] ?? [];
      if (!buffered.some((item) => item.id === event.id)) {
        liveBufferByTaskId = {
          ...liveBufferByTaskId,
          [event.task_id]: [...buffered, event],
        };
      }
    }
  }

  let commands = state.commands;
  if (isLifecycleEvent(event) && isTerminalTask(event.payload.task)) {
    commands = removeCancelCommand(state, event.task_id);
  }

  return {
    ...state,
    ...summary,
    selectedDetail,
    liveBufferByTaskId,
    detailStale,
    detailLoading,
    detailError,
    appliedEventId: event.id,
    commands,
  };
}

function installDetail(
  state: AgentState,
  taskId: string,
  generation: number,
  detail: TaskDetail,
): AgentState {
  if (state.selectedTaskId !== taskId || state.selectionGeneration !== generation) {
    return state;
  }

  const buffered = [...(state.liveBufferByTaskId[taskId] ?? [])]
    .filter((event) => event.id > detail.event_cursor)
    .sort((left, right) => left.id - right.id);
  let installed = detail;
  for (const [index, event] of buffered.entries()) {
    const projection = applyEventToDetail(installed, event);
    if (projection.kind === "conflict") {
      if (event.kind !== "review.updated") {
        return protocolError(state, "detail_projection_conflict");
      }
      return beginSnapshotRecovery(
        {
          ...state,
          selectedDetail: installed,
          detailLoading: false,
          detailStale: false,
        },
        event,
        projection.reason,
      );
    }
    if (projection.kind === "stale") {
      return {
        ...state,
        selectedDetail: projection.detail,
        detailLoading: true,
        detailError: null,
        detailStale: true,
        selectionGeneration: state.selectionGeneration + 1,
        liveBufferByTaskId: {
          ...state.liveBufferByTaskId,
          [taskId]: buffered.slice(index),
        },
      };
    }
    installed = projection.detail;
  }

  const existing = state.tasksById[taskId];
  const summary =
    existing !== undefined && existing.last_event_id > installed.task.last_event_id
      ? { tasksById: state.tasksById, taskOrder: state.taskOrder }
      : withTask(state, installed.task);

  return {
    ...state,
    ...summary,
    selectedDetail: installed,
    detailLoading: false,
    detailError: null,
    detailStale: false,
    liveBufferByTaskId: { ...state.liveBufferByTaskId, [taskId]: [] },
  };
}

function appendDiagnostic(
  state: AgentState,
  diagnostic: IgnoredEventDiagnostic,
): IgnoredEventDiagnostic[] {
  return [...state.diagnostics, diagnostic].slice(-MAX_DIAGNOSTICS);
}

function installBootstrapSnapshot(
  state: AgentState,
  bootstrap: BootstrapResponse,
): AgentState {
  const repositories = normalizeById(bootstrap.repositories);
  const tasks = normalizeById(bootstrap.tasks);
  const selectedTaskId =
    state.selectedTaskId !== null && state.selectedTaskId in tasks.byId
      ? state.selectedTaskId
      : null;
  const acceptServiceGeneration =
    bootstrap.service_state_generation >= state.serviceGeneration;
  return {
    ...state,
    repositoriesById: repositories.byId,
    repositoryOrder: repositories.order,
    tasksById: tasks.byId,
    taskOrder: tasks.order,
    selectedTaskId,
    selectedDetail: null,
    selectionGeneration:
      selectedTaskId === null
        ? state.selectionGeneration
        : state.selectionGeneration + 1,
    detailLoading: selectedTaskId !== null,
    detailError: null,
    detailStale: false,
    liveBufferByTaskId:
      selectedTaskId === null ? {} : { [selectedTaskId]: [] },
    snapshotRecovery: null,
    recoveryBuffer: [],
    appliedEventId: bootstrap.latest_event_id,
    serviceState: acceptServiceGeneration
      ? bootstrap.service_state
      : state.serviceState,
    serviceGeneration: acceptServiceGeneration
      ? bootstrap.service_state_generation
      : state.serviceGeneration,
    commands: reconcileCommands(state, tasks.byId),
    connection: "live",
    recoveryReason: null,
  };
}

function recoverFromBootstrap(
  state: AgentState,
  bootstrap: BootstrapResponse,
  bufferedEvents: TaskEvent[],
): AgentState {
  const combined = [...state.recoveryBuffer];
  for (const event of bufferedEvents) {
    if (!combined.some((candidate) => candidate.id === event.id)) {
      combined.push(event);
    }
  }

  let recovered = installBootstrapSnapshot(state, bootstrap);
  for (const event of combined
    .filter((candidate) => candidate.id > bootstrap.latest_event_id)
    .sort((left, right) => left.id - right.id)) {
    recovered = reducePersistedEvent(recovered, event);
    if (
      recovered.connection === "protocol_error" ||
      recovered.snapshotRecovery !== null
    ) {
      return {
        ...state,
        connection: "recovering",
        recoveryReason:
          state.snapshotRecovery?.reason ?? "snapshot_recovery_failed",
      };
    }
  }
  return recovered;
}

export function agentReducer(state: AgentState, action: AgentAction): AgentState {
  switch (action.type) {
    case "bootstrap.started":
      return {
        ...state,
        connection: "bootstrapping",
        recoveryReason: null,
      };
    case "bootstrap.received": {
      return installBootstrapSnapshot(state, action.bootstrap);
    }
    case "bootstrap.failed":
      return {
        ...state,
        connection: action.protocol ? "protocol_error" : "unavailable",
        recoveryReason: action.reason,
      };
    case "connection.changed":
      return {
        ...state,
        connection: action.connection,
        recoveryReason: action.reason,
      };
    case "session.expired":
      return {
        ...state,
        connection: "session_expired",
        recoveryReason: "session_expired",
        snapshotRecovery: null,
        recoveryBuffer: [],
        commands: { ...state.commands, cancelByTaskId: {} },
      };
    case "repository.upserted":
      return { ...state, ...withRepository(state, action.repository) };
    case "task.upserted":
      return upsertRestTask(state, action.task);
    case "task.selected":
      return {
        ...state,
        selectedTaskId: action.taskId,
        selectedDetail: null,
        selectionGeneration: state.selectionGeneration + 1,
        detailLoading: true,
        detailError: null,
        detailStale: false,
        liveBufferByTaskId: { [action.taskId]: [] },
      };
    case "detail.received":
      return installDetail(
        state,
        action.taskId,
        action.generation,
        action.detail,
      );
    case "detail.failed":
      if (
        state.selectedTaskId !== action.taskId ||
        state.selectionGeneration !== action.generation
      ) {
        return state;
      }
      return {
        ...state,
        detailLoading: false,
        detailError: action.message,
      };
    case "event.received":
      return reducePersistedEvent(state, action.event);
    case "event.conflicted":
      if (
        action.event.schema_version !== SUPPORTED_SCHEMA_VERSION ||
        action.event.id <= state.appliedEventId
      ) {
        return state;
      }
      return beginSnapshotRecovery(state, action.event, action.reason);
    case "recovery.received":
      if (state.snapshotRecovery === null) {
        return state;
      }
      return recoverFromBootstrap(
        state,
        action.bootstrap,
        action.bufferedEvents,
      );
    case "event.unknown": {
      if (action.schemaVersion !== SUPPORTED_SCHEMA_VERSION) {
        return protocolError(state, "unsupported_schema_version");
      }
      if (action.id === state.appliedEventId) {
        return state;
      }
      if (action.id < state.appliedEventId) {
        return protocolError(state, "non_monotonic_event_id");
      }
      return {
        ...state,
        appliedEventId: action.id,
        diagnostics: appendDiagnostic(state, {
          id: action.id,
          kind: action.kind,
          schemaVersion: action.schemaVersion,
          message: "Ignored an event kind that this client does not yet understand.",
        }),
      };
    }
    case "service.received":
      if (action.generation <= state.serviceGeneration) {
        return state;
      }
      return {
        ...state,
        serviceState: action.state,
        serviceGeneration: action.generation,
      };
    case "cancel.started":
      return {
        ...state,
        commands: {
          ...state.commands,
          cancelByTaskId: {
            ...state.commands.cancelByTaskId,
            [action.taskId]: { phase: "pending", optimistic: true, error: null },
          },
        },
      };
    case "cancel.succeeded": {
      const current = state.tasksById[action.response.task.id];
      const responseWouldRegressTerminal =
        current !== undefined &&
        isTerminalTask(current) &&
        !isTerminalTask(action.response.task);
      const responseIsOlder =
        current !== undefined &&
        action.response.task.last_event_id < current.last_event_id;
      const taskUpdate =
        responseWouldRegressTerminal || responseIsOlder
          ? { tasksById: state.tasksById, taskOrder: state.taskOrder }
          : withTask(state, action.response.task);
      const resultingTask = taskUpdate.tasksById[action.response.task.id];
      const commands = resultingTask !== undefined && isTerminalTask(resultingTask)
        ? removeCancelCommand(state, action.response.task.id)
        : state.commands;
      return { ...state, ...taskUpdate, commands };
    }
    case "cancel.failed": {
      const current = state.tasksById[action.taskId];
      if (current !== undefined && isTerminalTask(current)) {
        return {
          ...state,
          commands: removeCancelCommand(state, action.taskId),
        };
      }
      return {
        ...state,
        commands: {
          ...state.commands,
          cancelByTaskId: {
            ...state.commands.cancelByTaskId,
            [action.taskId]: {
              phase: "error",
              optimistic: false,
              error: action.error,
            },
          },
        },
      };
    }
  }
}
