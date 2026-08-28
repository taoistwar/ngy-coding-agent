import type { RefObject } from "react";

import type {
  DeliveryCleanupOperationEnvelope,
  DeliveryTask,
} from "../../api/types";
import { shouldPollDeliveryOperation } from "../../state/deliveryModel";
import type { DeliveryPollingController } from "../../state/useDeliveryPolling";
import { CleanupDialog } from "./CleanupDialog";
import {
  availableTarget,
  branchLabel,
  cleanupKindLabel,
  cleanupStateLabel,
  worktreeLabel,
} from "./cleanupModel";
import type { DeliveryPanelApi } from "./types";
import { useCleanupConfirmation } from "./useCleanupConfirmation";
import type { DeliveryCommandRunner } from "./useDeliveryCommandRunner";

export interface CleanupControlsProps {
  api: DeliveryPanelApi;
  controller: DeliveryPollingController;
  taskId: string;
  projection: DeliveryTask;
  operation: DeliveryCleanupOperationEnvelope | null;
  removeRunner: DeliveryCommandRunner;
  deleteRunner: DeliveryCommandRunner;
  panelFallbackRef: RefObject<HTMLElement | null>;
}

export function CleanupControls({
  api,
  controller,
  taskId,
  projection,
  operation,
  removeRunner,
  deleteRunner,
  panelFallbackRef,
}: CleanupControlsProps) {
  const disposition = projection.disposition;
  const hasRemoveAction = projection.allowed_actions.includes("remove_worktree");
  const hasDeleteAction = projection.allowed_actions.includes("delete_branch");
  const mutationPending = operation !== null && shouldPollDeliveryOperation(operation);
  const readyForNewAction = controller.state.phase === "ready" && !mutationPending;
  const confirmation = useCleanupConfirmation({
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
  });

  return (
    <>
      {disposition !== null ? (
        <section
          className="delivery-cleanup"
          aria-labelledby={`cleanup-heading-${taskId}`}
        >
          <h4 id={`cleanup-heading-${taskId}`}>Local artifacts after merge</h4>
          <p className="delivery-retained-default">
            The safe default is retention: the worktree and source branch remain until
            you explicitly request each cleanup step.
          </p>
          <dl className="delivery-disposition">
            <div>
              <dt>Worktree</dt>
              <dd>{worktreeLabel(disposition.worktree.state)}</dd>
            </div>
            <div>
              <dt>Source branch</dt>
              <dd>{branchLabel(disposition.branch.state)}</dd>
            </div>
          </dl>

          {operation !== null ? (
            <p role="status" aria-label="Cleanup operation status">
              {cleanupKindLabel(operation.cleanup_kind)}:{" "}
              {cleanupStateLabel(operation.state)}
              {operation.failure !== null ? (
                <>
                  {" "}(<code>{operation.failure.code}</code>)
                </>
              ) : null}
            </p>
          ) : null}

          <div
            className="delivery-cleanup-actions"
            aria-label="Local cleanup actions"
          >
            {hasRemoveAction ? (
              <button
                type="button"
                aria-haspopup="dialog"
                disabled={
                  !readyForNewAction || removeRunner.state.phase === "pending"
                }
                onClick={(event) => confirmation.openRemove(event.currentTarget)}
              >
                {removeRunner.state.phase === "pending"
                  ? "Removing worktree…"
                  : "Remove worktree"}
              </button>
            ) : null}
            {hasDeleteAction ? (
              <button
                type="button"
                aria-haspopup="dialog"
                disabled={
                  !readyForNewAction ||
                  availableTarget(projection) === null ||
                  deleteRunner.state.phase === "pending"
                }
                onClick={(event) => confirmation.openDelete(event.currentTarget)}
              >
                {deleteRunner.state.phase === "pending"
                  ? "Deleting source branch…"
                  : "Delete source branch"}
              </button>
            ) : null}
          </div>

          <CommandReceipt action="Remove worktree" runner={removeRunner} />
          <CommandReceipt action="Delete source branch" runner={deleteRunner} />
        </section>
      ) : null}
      {confirmation.dialog !== null ? (
        <CleanupDialog
          key={confirmation.dialog.scopeKey}
          snapshot={confirmation.dialog}
          fresh={confirmation.fresh}
          canSubmit={confirmation.canSubmit}
          runner={confirmation.runner}
          onExecute={confirmation.execute}
          onClose={confirmation.close}
        />
      ) : null}
    </>
  );
}

function CommandReceipt({
  action,
  runner,
}: {
  action: string;
  runner: DeliveryCommandRunner;
}) {
  if (runner.state.phase !== "succeeded") return null;
  return (
    <p className="delivery-cleanup-receipt" role="status">
      {action} receipt: {runner.state.response.receipt}; operation{" "}
      <code>{runner.state.response.operation.operation_id}</code>.
    </p>
  );
}
