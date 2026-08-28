import type { components } from "./generated/schema";

export type Repository = components["schemas"]["RepositoryDto"];
export type Task = components["schemas"]["TaskDto"];
export type TaskDetail = components["schemas"]["TaskDetailDto"];
export type TaskEvent = components["schemas"]["TaskEventDto"];
export type TaskEventKind = components["schemas"]["TaskEventKindDto"];
export type BootstrapResponse = components["schemas"]["BootstrapResponse"];
export type SchedulerState = components["schemas"]["SchedulerStateDto"];
export type SchedulerAdmissionState =
  components["schemas"]["SchedulerAdmissionStateDto"];
export type SchedulerLimits = components["schemas"]["SchedulerLimitsDto"];
export type SchedulerQueueReason =
  components["schemas"]["SchedulerQueueReasonDto"];
export type SchedulerQueuedTask =
  components["schemas"]["SchedulerQueuedTaskDto"];
export type SchedulerRepositoryStorage =
  components["schemas"]["SchedulerRepositoryStorageDto"];
export type SchedulerStopIntent =
  components["schemas"]["SchedulerStopIntentDto"];
export type SchedulerStoppingTask =
  components["schemas"]["SchedulerStoppingTaskDto"];
export type SchedulerStorage = components["schemas"]["SchedulerStorageDto"];
export type SchedulerStorageScope =
  components["schemas"]["SchedulerStorageScopeDto"];
export type SchedulerStorageState =
  components["schemas"]["SchedulerStorageStateDto"];
export type SchedulerControlStorage =
  components["schemas"]["SchedulerControlStorageDto"];
export type SchedulerStateControl =
  components["schemas"]["SchedulerStateControl"];
export type SchedulerStateChunkControl =
  components["schemas"]["SchedulerStateChunkControl"];
export type SchedulerStateItem =
  components["schemas"]["SchedulerStateItemDto"];
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
export type DeliveryAllowedAction =
  components["schemas"]["DeliveryAllowedActionDto"];
export type DeliveryArtifactDisposition =
  components["schemas"]["DeliveryArtifactDispositionDto"];
export type DeliveryCleanupKind =
  components["schemas"]["DeliveryCleanupKindDto"];
export type DeliveryCleanupOperation =
  components["schemas"]["DeliveryCleanupOperationDto"];
export type DeliveryCleanupOperationEnvelope =
  components["schemas"]["DeliveryCleanupOperationEnvelopeDto"];
export type DeliveryCleanupState =
  components["schemas"]["DeliveryCleanupStateDto"];
export type DeliveryCommandResponse =
  components["schemas"]["DeliveryCommandResponse"];
export type DeliveryConflictSummary =
  components["schemas"]["DeliveryConflictSummaryDto"];
export type DeliveryDeleteBranchRequest =
  components["schemas"]["DeliveryDeleteBranchRequest"];
export type DeliveryMergeOperation =
  components["schemas"]["DeliveryMergeOperationDto"];
export type DeliveryMergeOperationEnvelope =
  components["schemas"]["DeliveryMergeOperationEnvelopeDto"];
export type DeliveryMergeRequest =
  components["schemas"]["DeliveryMergeRequest"];
export type DeliveryMergeState =
  components["schemas"]["DeliveryMergeStateDto"];
export type DeliveryOperation =
  components["schemas"]["DeliveryOperationDto"];
export type DeliveryPreflightRequest =
  components["schemas"]["DeliveryPreflightRequest"];
export type DeliveryRemoveWorktreeRequest =
  components["schemas"]["DeliveryRemoveWorktreeRequest"];
export type DeliveryTask = components["schemas"]["DeliveryTaskDto"];
