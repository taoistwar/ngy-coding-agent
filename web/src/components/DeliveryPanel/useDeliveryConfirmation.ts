import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type RefObject,
} from "react";

import type { DeliveryClient } from "../../api/deliveryClient";
import type {
  DeliveryMergeOperationEnvelope,
  DeliveryOperation,
  DeliveryTask,
} from "../../api/types";
import type { DeliveryState } from "../../state/deliveryModel";
import type { DeliveryPollingController } from "../../state/useDeliveryPolling";
import { restoreDeliveryDialogFocus } from "./AccessibleDeliveryDialog";
import type { DeliveryConfirmationSnapshot } from "./PreflightModal";
import { refreshAfterSettledRejection } from "./commandSettlement";
import {
  authorityFromOperation,
  authorityFromProjection,
  authorityMatchesProjection,
  confirmationScope,
  currentMergeOperation,
  isOperationPending,
  operationSnapshot,
  sameAuthority,
  sameModal,
} from "./deliveryConfirmationModel";
import type { DeliveryPanelApi } from "./types";
import {
  prepareDeliveryCommandRunner,
  type DeliveryCommandRunner,
} from "./useDeliveryCommandRunner";

interface UseDeliveryConfirmationOptions {
  api: DeliveryPanelApi | DeliveryClient;
  controller: DeliveryPollingController;
  taskId: string;
  projection: DeliveryTask | null;
  operation: DeliveryOperation | null;
  phase: DeliveryState["phase"];
  preflightRunner: DeliveryCommandRunner;
  mergeRunner: DeliveryCommandRunner;
  panelFallbackRef: RefObject<HTMLElement | null>;
}

export interface DeliveryConfirmationController {
  dialog: DeliveryConfirmationSnapshot | null;
  dialogOperation: DeliveryMergeOperationEnvelope | null;
  mergeOperation: DeliveryMergeOperationEnvelope | null;
  fresh: boolean;
  canRunPreflight: boolean;
  canMerge: boolean;
  openPreflight(trigger: HTMLElement): void;
  openMerge(trigger: HTMLElement): void;
  runPreflight(): void;
  runMerge(): void;
  close(): void;
}

interface DialogState {
  dialog: DeliveryConfirmationSnapshot | null;
  dialogRef: { current: DeliveryConfirmationSnapshot | null };
  setDialog(next: DeliveryConfirmationSnapshot | null): void;
  close(): void;
}

export function useDeliveryConfirmation({
  api,
  controller,
  taskId,
  projection,
  operation,
  phase,
  preflightRunner,
  mergeRunner,
  panelFallbackRef,
}: UseDeliveryConfirmationOptions): DeliveryConfirmationController {
  const { dialog, dialogRef, setDialog, close } = useDialogState(
    taskId,
    controller,
    panelFallbackRef,
  );
  const mergeOperation = currentMergeOperation(projection, operation);
  const openPreflight = useOpenPreflight({
    controller,
    taskId,
    projection,
    operation,
    phase,
    runner: preflightRunner,
    setDialog,
  });
  const openMerge = useOpenMerge({
    controller,
    taskId,
    projection,
    mergeOperation,
    phase,
    runner: mergeRunner,
    setDialog,
  });
  const runPreflight = useRunPreflight({
    api,
    controller,
    taskId,
    runner: preflightRunner,
    dialogRef,
    setDialog,
  });
  const runMerge = useRunMerge({
    api,
    controller,
    taskId,
    runner: mergeRunner,
    dialogRef,
    setDialog,
  });

  const fresh =
    dialog !== null &&
    projection !== null &&
    controller.state.taskId === taskId &&
    sameModal(controller.state.modal, dialog) &&
    authorityMatchesProjection(dialog.authority, projection);
  const dialogOperation =
    dialog?.operationId !== null &&
    mergeOperation?.operation_id === dialog?.operationId
      ? mergeOperation
      : dialog?.operation ?? null;
  const canRunPreflight =
    dialog?.kind === "preflight" &&
    fresh &&
    phase === "ready" &&
    projection?.allowed_actions.includes("run_preflight") === true &&
    !isOperationPending(operation);
  const canMerge =
    dialog?.kind === "merge" &&
    fresh &&
    phase === "ready" &&
    projection?.allowed_actions.includes("accept_merge") === true &&
    dialog.operationId !== null &&
    dialog.operationVersion !== null &&
    mergeOperation?.operation_id === dialog.operationId &&
    mergeOperation.version === dialog.operationVersion &&
    mergeOperation.state === "preflight_ready";

  return {
    dialog,
    dialogOperation,
    mergeOperation,
    fresh,
    canRunPreflight,
    canMerge,
    openPreflight,
    openMerge,
    runPreflight,
    runMerge,
    close,
  };
}

function useDialogState(
  taskId: string,
  controller: DeliveryPollingController,
  panelFallbackRef: RefObject<HTMLElement | null>,
): DialogState {
  const [dialog, setDialogState] = useState<DeliveryConfirmationSnapshot | null>(
    null,
  );
  const dialogRef = useRef(dialog);
  const setDialog = useCallback(
    (next: DeliveryConfirmationSnapshot | null) => {
      dialogRef.current = next;
      setDialogState(next);
    },
    [],
  );

  useEffect(() => {
    setDialog(null);
  }, [setDialog, taskId]);

  const close = useCallback(() => {
    const returnFocus = dialogRef.current?.returnFocus ?? null;
    controller.clearModal();
    setDialog(null);
    restoreDeliveryDialogFocus(returnFocus, panelFallbackRef.current);
  }, [controller, panelFallbackRef, setDialog]);

  return { dialog, dialogRef, setDialog, close };
}

function useOpenPreflight({
  controller,
  taskId,
  projection,
  operation,
  phase,
  runner,
  setDialog,
}: {
  controller: DeliveryPollingController;
  taskId: string;
  projection: DeliveryTask | null;
  operation: DeliveryOperation | null;
  phase: DeliveryState["phase"];
  runner: DeliveryCommandRunner;
  setDialog(next: DeliveryConfirmationSnapshot | null): void;
}): (trigger: HTMLElement) => void {
  return useCallback(
    (trigger: HTMLElement) => {
      if (
        projection === null ||
        phase !== "ready" ||
        !projection.allowed_actions.includes("run_preflight") ||
        isOperationPending(operation) ||
        runner.state.phase === "pending"
      ) {
        return;
      }
      const authority = authorityFromProjection(projection);
      if (authority === null) return;
      const scopeKey = confirmationScope(
        "preflight",
        taskId,
        authority,
        null,
        null,
      );
      prepareDeliveryCommandRunner(runner, scopeKey);
      const snapshot: DeliveryConfirmationSnapshot = {
        kind: "preflight",
        taskId,
        operationId: null,
        operationVersion: null,
        authority,
        sourceState: projection.source?.state ?? null,
        sourceRef: projection.source?.source_ref ?? null,
        sourceOid: projection.source?.source_oid ?? null,
        operation: null,
        scopeKey,
        returnFocus: trigger,
      };
      setDialog(snapshot);
      controller.openModal({
        kind: "preflight",
        operationId: null,
        operationVersion: null,
        authority,
      });
    },
    [controller, operation, phase, projection, runner, setDialog, taskId],
  );
}

function useOpenMerge({
  controller,
  taskId,
  projection,
  mergeOperation,
  phase,
  runner,
  setDialog,
}: {
  controller: DeliveryPollingController;
  taskId: string;
  projection: DeliveryTask | null;
  mergeOperation: DeliveryMergeOperationEnvelope | null;
  phase: DeliveryState["phase"];
  runner: DeliveryCommandRunner;
  setDialog(next: DeliveryConfirmationSnapshot | null): void;
}): (trigger: HTMLElement) => void {
  return useCallback(
    (trigger: HTMLElement) => {
      if (
        projection === null ||
        phase !== "ready" ||
        mergeOperation === null ||
        mergeOperation.state !== "preflight_ready" ||
        !projection.allowed_actions.includes("accept_merge") ||
        runner.state.phase === "pending"
      ) {
        return;
      }
      const projectionAuthority = authorityFromProjection(projection);
      const authority = authorityFromOperation(mergeOperation);
      if (
        projectionAuthority === null ||
        !sameAuthority(projectionAuthority, authority)
      ) {
        return;
      }
      const scopeKey = confirmationScope(
        "merge",
        taskId,
        authority,
        mergeOperation.operation_id,
        mergeOperation.version,
      );
      prepareDeliveryCommandRunner(runner, scopeKey);
      const snapshot = operationSnapshot(
        "merge",
        taskId,
        authority,
        projection,
        mergeOperation,
        scopeKey,
        trigger,
      );
      setDialog(snapshot);
      controller.openModal({
        kind: "merge",
        operationId: mergeOperation.operation_id,
        operationVersion: mergeOperation.version,
        authority,
      });
    },
    [controller, mergeOperation, phase, projection, runner, setDialog, taskId],
  );
}

function useRunPreflight({
  api,
  controller,
  taskId,
  runner,
  dialogRef,
  setDialog,
}: {
  api: DeliveryPanelApi | DeliveryClient;
  controller: DeliveryPollingController;
  taskId: string;
  runner: DeliveryCommandRunner;
  dialogRef: DialogState["dialogRef"];
  setDialog: DialogState["setDialog"];
}): () => void {
  return useCallback(() => {
    const snapshot = dialogRef.current;
    const currentProjection = controller.state.projection;
    if (
      snapshot === null ||
      snapshot.kind !== "preflight" ||
      controller.state.taskId !== taskId ||
      controller.state.phase !== "ready" ||
      currentProjection === null ||
      !currentProjection.allowed_actions.includes("run_preflight") ||
      !sameModal(controller.state.modal, snapshot) ||
      !authorityMatchesProjection(snapshot.authority, currentProjection) ||
      isOperationPending(controller.state.operation)
    ) {
      return;
    }
    if (
      runner.state.phase === "error" &&
      runner.state.scopeKey === snapshot.scopeKey
    ) {
      runner.retry();
      return;
    }
    if (runner.state.phase !== "idle") return;
    const command = api.newPreflight(taskId, {
      target_branch: snapshot.authority.targetBranch,
      expected_target_head: snapshot.authority.targetHead,
    });
    runner.start(snapshot.scopeKey, command, {
      onSuccess: (response) => {
        controller.trackOperation(response.operation);
        const current = dialogRef.current;
        if (
          response.operation.kind !== "merge" ||
          current === null ||
          current.kind !== "preflight" ||
          current.scopeKey !== snapshot.scopeKey
        ) {
          return;
        }
        const responseAuthority = authorityFromOperation(response.operation);
        const next = operationSnapshot(
          "preflight",
          taskId,
          responseAuthority,
          currentProjection,
          response.operation,
          current.scopeKey,
          current.returnFocus,
        );
        setDialog(next);
        controller.openModal({
          kind: "preflight",
          operationId: response.operation.operation_id,
          operationVersion: response.operation.version,
          authority: responseAuthority,
        });
      },
      onFailure: (error) =>
        refreshAfterSettledRejection(controller, taskId, error),
    });
  }, [api, controller, dialogRef, runner, setDialog, taskId]);
}

function useRunMerge({
  api,
  controller,
  taskId,
  runner,
  dialogRef,
  setDialog,
}: {
  api: DeliveryPanelApi | DeliveryClient;
  controller: DeliveryPollingController;
  taskId: string;
  runner: DeliveryCommandRunner;
  dialogRef: DialogState["dialogRef"];
  setDialog: DialogState["setDialog"];
}): () => void {
  return useCallback(() => {
    const snapshot = dialogRef.current;
    const currentProjection = controller.state.projection;
    const currentOperation = currentMergeOperation(
      currentProjection,
      controller.state.operation,
    );
    if (
      snapshot === null ||
      snapshot.kind !== "merge" ||
      snapshot.operationId === null ||
      snapshot.operationVersion === null ||
      controller.state.taskId !== taskId ||
      controller.state.phase !== "ready" ||
      currentProjection === null ||
      !currentProjection.allowed_actions.includes("accept_merge") ||
      !sameModal(controller.state.modal, snapshot) ||
      !authorityMatchesProjection(snapshot.authority, currentProjection) ||
      currentOperation?.operation_id !== snapshot.operationId ||
      currentOperation.version !== snapshot.operationVersion ||
      currentOperation.state !== "preflight_ready"
    ) {
      return;
    }
    if (
      runner.state.phase === "error" &&
      runner.state.scopeKey === snapshot.scopeKey
    ) {
      runner.retry();
      return;
    }
    if (runner.state.phase !== "idle") return;
    const command = api.newMerge(taskId, {
      preflight_operation_id: snapshot.operationId,
      expected_operation_version: snapshot.operationVersion,
      expected_review_generation: snapshot.authority.reviewGeneration,
      expected_workspace_fingerprint: snapshot.authority.workspaceFingerprint,
      target_branch: snapshot.authority.targetBranch,
      expected_target_head: snapshot.authority.targetHead,
    });
    runner.start(snapshot.scopeKey, command, {
      onSuccess: (response) => {
        controller.trackOperation(response.operation);
        const current = dialogRef.current;
        if (
          response.operation.kind !== "merge" ||
          current === null ||
          current.kind !== "merge" ||
          current.scopeKey !== snapshot.scopeKey
        ) {
          return;
        }
        const responseAuthority = authorityFromOperation(response.operation);
        const next = operationSnapshot(
          "merge",
          taskId,
          responseAuthority,
          currentProjection,
          response.operation,
          current.scopeKey,
          current.returnFocus,
        );
        setDialog(next);
        controller.openModal({
          kind: "merge",
          operationId: response.operation.operation_id,
          operationVersion: response.operation.version,
          authority: responseAuthority,
        });
      },
      onFailure: (error) =>
        refreshAfterSettledRejection(controller, taskId, error),
    });
  }, [api, controller, dialogRef, runner, setDialog, taskId]);
}
