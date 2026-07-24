import type {
  BootstrapResponse,
  Repository,
  Task,
  TaskDetail,
  TaskEvent,
} from "../api/types";

export type AgentConnectionState =
  | "bootstrapping"
  | "live"
  | "reconnecting"
  | "recovering"
  | "unavailable"
  | "protocol_error"
  | "session_expired";

export interface IgnoredEventDiagnostic {
  id: number;
  kind: string;
  schemaVersion: number;
  message: string;
}

export interface CommandError {
  code: string;
  message: string;
  retryable: boolean;
  requestId: string | null;
}

export type CancelCommandState =
  | {
      phase: "pending";
      optimistic: true;
      error: null;
    }
  | {
      phase: "error";
      optimistic: false;
      error: CommandError;
    };

export interface EphemeralCommands {
  cancelByTaskId: Record<string, CancelCommandState>;
}

export interface SnapshotRecoveryState {
  conflictEventId: number;
  reason:
    | "review_payload_conflict"
    | "review_history_conflict"
    | "review_task_conflict";
}

export interface AgentState {
  repositoriesById: Record<string, Repository>;
  repositoryOrder: string[];
  tasksById: Record<string, Task>;
  taskOrder: string[];
  selectedTaskId: string | null;
  selectedDetail: TaskDetail | null;
  selectionGeneration: number;
  detailLoading: boolean;
  detailError: string | null;
  detailStale: boolean;
  liveBufferByTaskId: Record<string, TaskEvent[]>;
  snapshotRecovery: SnapshotRecoveryState | null;
  recoveryBuffer: TaskEvent[];
  appliedEventId: number;
  serviceState: BootstrapResponse["service_state"] | null;
  serviceGeneration: number;
  diagnostics: IgnoredEventDiagnostic[];
  commands: EphemeralCommands;
  connection: AgentConnectionState;
  recoveryReason: string | null;
}

export const initialAgentState: AgentState = {
  repositoriesById: {},
  repositoryOrder: [],
  tasksById: {},
  taskOrder: [],
  selectedTaskId: null,
  selectedDetail: null,
  selectionGeneration: 0,
  detailLoading: false,
  detailError: null,
  detailStale: false,
  liveBufferByTaskId: {},
  snapshotRecovery: null,
  recoveryBuffer: [],
  appliedEventId: 0,
  serviceState: null,
  serviceGeneration: 0,
  diagnostics: [],
  commands: { cancelByTaskId: {} },
  connection: "bootstrapping",
  recoveryReason: null,
};
