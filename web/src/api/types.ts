import type { components } from "./generated/schema";

export type Repository = components["schemas"]["RepositoryDto"];
export type Task = components["schemas"]["TaskDto"];
export type TaskDetail = components["schemas"]["TaskDetailDto"];
export type TaskEvent = components["schemas"]["TaskEventDto"];
export type BootstrapResponse = components["schemas"]["BootstrapResponse"];
export type ApiErrorResponse = components["schemas"]["ApiErrorResponse"];
export type AddRepositoryRequest = components["schemas"]["AddRepositoryRequest"];
export type CreateTaskRequest = components["schemas"]["CreateTaskRequest"];
export type CancellationAcceptedResponse =
  components["schemas"]["CancellationAcceptedResponse"];
export type SessionExchangeRequest = components["schemas"]["SessionExchangeRequest"];
export type QuitResponse = components["schemas"]["QuitResponse"];
export type PlanSnapshot = components["schemas"]["PlanSnapshotDto"];
export type DiffSnapshot = components["schemas"]["DiffSnapshotDto"];
export type TestSnapshot = components["schemas"]["TestSnapshotDto"];
export type ActivityEntry = components["schemas"]["ActivityEntryDto"];
export type TimelineEntry = components["schemas"]["TimelineEntryDto"];
export type SseMessage = components["schemas"]["SseMessage"];
export type ServiceState = components["schemas"]["ServiceStateDto"];
export type StreamReset = components["schemas"]["StreamResetControl"];
