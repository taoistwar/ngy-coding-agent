import type {
  DeliveryMergeOperationEnvelope,
  DeliveryOperation,
  DeliveryTask,
} from "../../api/types";
import {
  mergeEnvelopeFromTask,
  shouldPollDeliveryOperation,
  type DeliveryModalAuthority,
  type DeliveryModalState,
} from "../../state/deliveryModel";
import type { DeliveryConfirmationSnapshot } from "./PreflightModal";

export function authorityFromProjection(
  projection: DeliveryTask,
): DeliveryModalAuthority | null {
  if (
    projection.evidence === null ||
    projection.target.available !== true ||
    !("branch" in projection.target)
  ) {
    return null;
  }
  return {
    reviewGeneration: projection.evidence.review_generation,
    workspaceFingerprint: projection.evidence.workspace_fingerprint,
    targetBranch: projection.target.branch,
    targetHead: projection.target.head,
  };
}

export function authorityFromOperation(
  operation: DeliveryMergeOperationEnvelope,
): DeliveryModalAuthority {
  return {
    reviewGeneration: operation.review_generation,
    workspaceFingerprint: operation.workspace_fingerprint,
    targetBranch: operation.target_branch,
    targetHead: operation.target_head,
  };
}

export function authorityMatchesProjection(
  authority: DeliveryModalAuthority,
  projection: DeliveryTask,
): boolean {
  const current = authorityFromProjection(projection);
  return current !== null && sameAuthority(authority, current);
}

export function sameAuthority(
  left: DeliveryModalAuthority,
  right: DeliveryModalAuthority,
): boolean {
  return (
    left.reviewGeneration === right.reviewGeneration &&
    left.workspaceFingerprint === right.workspaceFingerprint &&
    left.targetBranch === right.targetBranch &&
    left.targetHead === right.targetHead
  );
}

export function sameModal(
  modal: DeliveryModalState | null,
  snapshot: DeliveryConfirmationSnapshot,
): boolean {
  return (
    modal !== null &&
    modal.kind === snapshot.kind &&
    modal.taskId === snapshot.taskId &&
    modal.operationId === snapshot.operationId &&
    modal.operationVersion === snapshot.operationVersion &&
    modal.authority !== null &&
    sameAuthority(modal.authority, snapshot.authority)
  );
}

export function operationSnapshot(
  kind: "preflight" | "merge",
  taskId: string,
  authority: DeliveryModalAuthority,
  projection: DeliveryTask,
  operation: DeliveryMergeOperationEnvelope,
  scopeKey: string,
  returnFocus: HTMLElement | null,
): DeliveryConfirmationSnapshot {
  return {
    kind,
    taskId,
    operationId: operation.operation_id,
    operationVersion: operation.version,
    authority,
    sourceState: projection.source?.state ?? null,
    sourceRef: projection.source?.source_ref ?? null,
    sourceOid:
      operation.source_commit ??
      operation.preflight_source_commit ??
      projection.source?.source_oid ??
      null,
    operation,
    scopeKey,
    returnFocus,
  };
}

export function confirmationScope(
  kind: "preflight" | "merge",
  taskId: string,
  authority: DeliveryModalAuthority,
  operationId: string | null,
  operationVersion: number | null,
): string {
  return [
    kind,
    taskId,
    authority.reviewGeneration,
    authority.workspaceFingerprint,
    authority.targetBranch,
    authority.targetHead,
    operationId ?? "none",
    operationVersion ?? "none",
  ].join("\u0000");
}

export function currentMergeOperation(
  projection: DeliveryTask | null,
  operation: DeliveryOperation | null,
): DeliveryMergeOperationEnvelope | null {
  const projected = projection === null ? null : mergeEnvelopeFromTask(projection);
  if (operation?.kind !== "merge") {
    return projected?.kind === "merge" ? projected : null;
  }
  if (
    projected?.kind === "merge" &&
    projected.operation_id === operation.operation_id &&
    projected.version > operation.version
  ) {
    return projected;
  }
  return operation;
}

export function isOperationPending(
  operation: DeliveryOperation | null,
): boolean {
  return operation !== null && shouldPollDeliveryOperation(operation);
}
