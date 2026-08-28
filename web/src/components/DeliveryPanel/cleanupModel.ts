import type {
  DeliveryCleanupOperationEnvelope,
  DeliveryTask,
} from "../../api/types";
import type { DeliveryPollingController } from "../../state/useDeliveryPolling";

export type CleanupDialogKind = "remove_worktree" | "delete_branch";

export interface CleanupSnapshot {
  kind: CleanupDialogKind;
  taskId: string;
  scopeKey: string;
  dispositionVersion: number;
  mergedOperationId: string;
  sourceRef: string;
  sourceOid: string;
  targetBranch: string | null;
  targetHead: string | null;
  returnFocus: HTMLElement | null;
}

export function createCleanupSnapshot(
  kind: CleanupDialogKind,
  taskId: string,
  dispositionVersion: number,
  disposition: NonNullable<DeliveryTask["disposition"]>,
  targetBranch: string | null,
  targetHead: string | null,
  returnFocus: HTMLElement,
): CleanupSnapshot {
  const values = [
    kind,
    taskId,
    dispositionVersion,
    disposition.merged_operation_id,
    disposition.source_ref,
    disposition.source_oid,
    targetBranch ?? "none",
    targetHead ?? "none",
  ];
  return {
    kind,
    taskId,
    scopeKey: values.join("\u0000"),
    dispositionVersion,
    mergedOperationId: disposition.merged_operation_id,
    sourceRef: disposition.source_ref,
    sourceOid: disposition.source_oid,
    targetBranch,
    targetHead,
    returnFocus,
  };
}

export function cleanupModalMatches(
  controller: DeliveryPollingController,
  snapshot: CleanupSnapshot,
): boolean {
  const modal = controller.state.modal;
  return (
    modal !== null &&
    modal.kind === snapshot.kind &&
    modal.taskId === snapshot.taskId &&
    modal.operationId === null &&
    modal.operationVersion === null &&
    modal.authority === null
  );
}

export function cleanupSnapshotMatches(
  snapshot: CleanupSnapshot,
  projection: DeliveryTask,
): boolean {
  const disposition = projection.disposition;
  if (disposition === null || projection.task_id !== snapshot.taskId) return false;
  const action =
    snapshot.kind === "remove_worktree" ? "remove_worktree" : "delete_branch";
  if (!projection.allowed_actions.includes(action)) return false;
  const version =
    snapshot.kind === "remove_worktree"
      ? disposition.worktree.version
      : disposition.branch.version;
  if (
    version !== snapshot.dispositionVersion ||
    disposition.merged_operation_id !== snapshot.mergedOperationId ||
    disposition.source_ref !== snapshot.sourceRef ||
    disposition.source_oid !== snapshot.sourceOid
  ) {
    return false;
  }
  if (snapshot.kind === "remove_worktree") return true;
  const target = availableTarget(projection);
  return (
    target !== null &&
    target.branch === snapshot.targetBranch &&
    target.head === snapshot.targetHead
  );
}

export function availableTarget(
  projection: DeliveryTask,
): { branch: string; head: string } | null {
  return projection.target.available === true && "branch" in projection.target
    ? { branch: projection.target.branch, head: projection.target.head }
    : null;
}

export function requireCleanupTarget(value: string | null): string {
  if (value === null) throw new Error("missing exact cleanup target");
  return value;
}

export function worktreeLabel(
  state: NonNullable<DeliveryTask["disposition"]>["worktree"]["state"],
): string {
  switch (state) {
    case "retained_locked":
      return "Retained and locked (default)";
    case "retained_unlocked":
      return "Retained and unlocked while removal continues";
    case "removed":
      return "Removed";
    case "reconciliation_required":
      return "Retained; reconciliation required";
  }
}

export function branchLabel(
  state: NonNullable<DeliveryTask["disposition"]>["branch"]["state"],
): string {
  switch (state) {
    case "retained":
      return "Retained (default)";
    case "deleted":
      return "Deleted";
    case "reconciliation_required":
      return "Retained; reconciliation required";
  }
}

export function cleanupKindLabel(kind: CleanupDialogKind): string {
  return kind === "remove_worktree" ? "Remove worktree" : "Delete source branch";
}

export function cleanupStateLabel(
  state: DeliveryCleanupOperationEnvelope["state"],
): string {
  switch (state) {
    case "unlock_pending":
      return "unlock pending";
    case "unlocked_pending_remove":
      return "unlocked; removal queued";
    case "remove_pending":
      return "removal pending";
    case "delete_pending":
      return "deletion pending";
    case "completed":
      return "completed";
    case "failed":
      return "failed without an unproven side effect";
    case "reconciliation_required":
      return "reconciliation required";
  }
}
