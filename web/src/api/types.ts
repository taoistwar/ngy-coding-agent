import type { components } from "./generated/schema";

export type Repository = components["schemas"]["RepositoryDto"];
export type Task = components["schemas"]["TaskDto"];
export type TaskDetail = components["schemas"]["TaskDetailDto"];
export type TaskEvent = components["schemas"]["TaskEventDto"];
export type TaskEventKind = components["schemas"]["TaskEventKindDto"];
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
export type ActivityActor = components["schemas"]["ActivityActorDto"];
export type TimelineEntry = components["schemas"]["TimelineEntryDto"];
export type DeliveryReadiness =
  components["schemas"]["DeliveryReadinessDto"];
export type WorkspaceDigest = components["schemas"]["WorkspaceDigestDto"];
export type WorkspaceDigestAlgorithm =
  components["schemas"]["WorkspaceDigestAlgorithmDto"];
export type CargoCheck = components["schemas"]["CargoCheckDto"];
export type CargoTest = components["schemas"]["CargoTestDto"];
export type RequiredCheck = components["schemas"]["RequiredCheckDto"];
export type CheckEvidence = components["schemas"]["CheckEvidenceDto"];
export type CheckActor = components["schemas"]["CheckActorDto"];
export type CheckEvidenceStatus =
  components["schemas"]["CheckEvidenceStatusDto"];
export type ReviewFinding = components["schemas"]["ReviewFindingDto"];
export type FindingSeverity = components["schemas"]["FindingSeverityDto"];
export type ReviewChunkIndex = components["schemas"]["ReviewChunkIndexDto"];
export type ReviewCoverage = components["schemas"]["ReviewCoverageDto"];
export type ReviewDecisionSource =
  components["schemas"]["ReviewDecisionSourceDto"];
export type ReviewVerdict = components["schemas"]["ReviewVerdictDto"];
export type ReviewEvidence = components["schemas"]["ReviewEvidenceDto"];
export type SseMessage = components["schemas"]["SseMessage"];
export type ServiceState = components["schemas"]["ServiceStateDto"];
export type StreamReset = components["schemas"]["StreamResetControl"];
