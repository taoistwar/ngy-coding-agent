import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type RefObject,
} from "react";

import type { DeliveryTask } from "../../api/types";
import { shouldPollDeliveryOperation } from "../../state/deliveryModel";
import type { DeliveryPollingController } from "../../state/useDeliveryPolling";
import { restoreDeliveryDialogFocus } from "./AccessibleDeliveryDialog";
import { refreshAfterSettledRejection } from "./commandSettlement";
import type { DeliveryPanelApi } from "./types";
import type { DeliveryCommandRunner } from "./useDeliveryCommandRunner";
import {
  availableTarget,
  cleanupModalMatches,
  cleanupSnapshotMatches,
  createCleanupSnapshot,
  requireCleanupTarget,
  type CleanupSnapshot,
} from "./cleanupModel";

interface UseCleanupConfirmationOptions {
  api: DeliveryPanelApi;
  controller: DeliveryPollingController;
  taskId: string;
  projection: DeliveryTask;
  removeRunner: DeliveryCommandRunner;
  deleteRunner: DeliveryCommandRunner;
  panelFallbackRef: RefObject<HTMLElement | null>;
  readyForNewAction: boolean;
  hasRemoveAction: boolean;
  hasDeleteAction: boolean;
}

export interface CleanupConfirmationController {
  dialog: CleanupSnapshot | null;
  fresh: boolean;
  canSubmit: boolean;
  runner: DeliveryCommandRunner;
  openRemove(trigger: HTMLElement): void;
  openDelete(trigger: HTMLElement): void;
  close(): void;
  execute(): void;
}

export function useCleanupConfirmation({
  api,
  controller,
  taskId,
  projection,
  removeRunner,
  deleteRunner,
  panelFallbackRef,
  readyForNewAction,
  hasRemoveAction,
  hasDeleteAction,
}: UseCleanupConfirmationOptions): CleanupConfirmationController {
  const [dialog, setDialogState] = useState<CleanupSnapshot | null>(null);
  const dialogRef = useRef(dialog);
  const disposition = projection.disposition;

  const setDialog = useCallback((next: CleanupSnapshot | null) => {
    dialogRef.current = next;
    setDialogState(next);
  }, []);

  useEffect(() => {
    setDialog(null);
  }, [setDialog, taskId]);

  const openRemove = (trigger: HTMLElement) => {
    if (
      disposition === null ||
      !hasRemoveAction ||
      !readyForNewAction ||
      removeRunner.state.phase === "pending"
    ) {
      return;
    }
    const snapshot = createCleanupSnapshot(
      "remove_worktree",
      taskId,
      disposition.worktree.version,
      disposition,
      null,
      null,
      trigger,
    );
    prepareRunner(removeRunner, snapshot.scopeKey);
    setDialog(snapshot);
    controller.openModal({
      kind: "remove_worktree",
      operationId: null,
      operationVersion: null,
      authority: null,
    });
  };

  const openDelete = (trigger: HTMLElement) => {
    if (
      disposition === null ||
      !hasDeleteAction ||
      !readyForNewAction ||
      deleteRunner.state.phase === "pending"
    ) {
      return;
    }
    const target = availableTarget(projection);
    if (target === null) return;
    const snapshot = createCleanupSnapshot(
      "delete_branch",
      taskId,
      disposition.branch.version,
      disposition,
      target.branch,
      target.head,
      trigger,
    );
    prepareRunner(deleteRunner, snapshot.scopeKey);
    setDialog(snapshot);
    controller.openModal({
      kind: "delete_branch",
      operationId: null,
      operationVersion: null,
      authority: null,
    });
  };

  const close = () => {
    const returnFocus = dialogRef.current?.returnFocus ?? null;
    controller.clearModal();
    setDialog(null);
    restoreDeliveryDialogFocus(returnFocus, panelFallbackRef.current);
  };

  const fresh =
    dialog !== null &&
    cleanupModalMatches(controller, dialog) &&
    cleanupSnapshotMatches(dialog, projection);
  const canSubmit = fresh && readyForNewAction;
  const runner = dialog?.kind === "delete_branch" ? deleteRunner : removeRunner;

  const execute = () => {
    const snapshot = dialogRef.current;
    if (
      snapshot === null ||
      !cleanupModalMatches(controller, snapshot) ||
      !cleanupSnapshotMatches(snapshot, projection) ||
      controller.state.phase !== "ready" ||
      (controller.state.operation !== null &&
        shouldPollDeliveryOperation(controller.state.operation))
    ) {
      return;
    }
    const commandRunner =
      snapshot.kind === "delete_branch" ? deleteRunner : removeRunner;
    if (
      commandRunner.state.phase === "error" &&
      commandRunner.state.scopeKey === snapshot.scopeKey
    ) {
      commandRunner.retry();
      return;
    }
    if (commandRunner.state.phase !== "idle") return;
    const command =
      snapshot.kind === "remove_worktree"
        ? api.newRemoveWorktree(taskId, {
            expected_disposition_version: snapshot.dispositionVersion,
            expected_merge_operation_id: snapshot.mergedOperationId,
            expected_source_ref: snapshot.sourceRef,
            expected_source_oid: snapshot.sourceOid,
          })
        : api.newDeleteBranch(taskId, {
            expected_disposition_version: snapshot.dispositionVersion,
            expected_merge_operation_id: snapshot.mergedOperationId,
            expected_source_ref: snapshot.sourceRef,
            expected_source_oid: snapshot.sourceOid,
            target_branch: requireCleanupTarget(snapshot.targetBranch),
            target_head: requireCleanupTarget(snapshot.targetHead),
          });
    commandRunner.start(snapshot.scopeKey, command, {
      onSuccess: (response) => controller.trackOperation(response.operation),
      onFailure: (error) =>
        refreshAfterSettledRejection(controller, taskId, error),
    });
  };

  return {
    dialog,
    fresh,
    canSubmit,
    runner,
    openRemove,
    openDelete,
    close,
    execute,
  };
}

function prepareRunner(runner: DeliveryCommandRunner, scopeKey: string): void {
  if (
    runner.state.phase === "error" &&
    runner.state.scopeKey === scopeKey &&
    runner.state.error.retryable
  ) {
    return;
  }
  runner.reset();
}
