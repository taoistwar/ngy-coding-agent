import { useRef } from "react";

import { CleanupControls } from "./CleanupControls";
import { Eligibility } from "./Eligibility";
import { MergeProgress } from "./MergeProgress";
import { PreflightModal } from "./PreflightModal";
import { isOperationPending } from "./deliveryConfirmationModel";
import type { DeliveryPanelProps } from "./types";
import { useDeliveryCommandRunner } from "./useDeliveryCommandRunner";
import { useDeliveryConfirmation } from "./useDeliveryConfirmation";

export type {
  DeliveryPanelApi,
  DeliveryPanelBinding,
  DeliveryPanelProps,
} from "./types";
export type {
  DeliveryCommandExecutionState,
  DeliveryCommandRunner,
} from "./useDeliveryCommandRunner";

export function DeliveryPanel({ api, controller, taskId }: DeliveryPanelProps) {
  const panelRef = useRef<HTMLElement>(null);
  const preflightRunner = useDeliveryCommandRunner(taskId);
  const mergeRunner = useDeliveryCommandRunner(taskId);
  const removeRunner = useDeliveryCommandRunner(taskId);
  const deleteRunner = useDeliveryCommandRunner(taskId);
  const stateMatchesTask = controller.state.taskId === taskId;
  const projection =
    stateMatchesTask && controller.state.projection?.task_id === taskId
      ? controller.state.projection
      : null;
  const operation = stateMatchesTask ? controller.state.operation : null;
  const cleanupOperation = operation?.kind === "cleanup" ? operation : null;
  const phase = stateMatchesTask ? controller.state.phase : "loading";
  const error = stateMatchesTask ? controller.state.error : null;
  const confirmation = useDeliveryConfirmation({
    api,
    controller,
    taskId,
    projection,
    operation,
    phase,
    preflightRunner,
    mergeRunner,
    panelFallbackRef: panelRef,
  });
  const actionBusy =
    phase !== "ready" ||
    isOperationPending(operation) ||
    preflightRunner.state.phase === "pending" ||
    mergeRunner.state.phase === "pending";

  return (
    <>
      <section
        ref={panelRef}
        className="delivery-panel evidence-panel"
        aria-labelledby={`delivery-heading-${taskId}`}
        tabIndex={-1}
      >
        <div className="panel-heading-row">
          <div>
            <p className="eyebrow">Explicit local delivery</p>
            <h3 id={`delivery-heading-${taskId}`}>Delivery</h3>
          </div>
          {phase === "loading" || phase === "refreshing" ? (
            <span role="status" aria-label="Loading delivery status">
              Refreshing…
            </span>
          ) : null}
        </div>
        <p className="delivery-boundary-note">
          Delivery merges into the exact local target shown here. It does not publish,
          deploy, or change a remote branch.
        </p>

        {error !== null ? (
          <div className="delivery-load-error" role="alert">
            <p>{error.message}</p>
            <p>
              Error code: <code>{error.code}</code>
            </p>
            {error.requestId !== null ? (
              <p>
                Request ID: <code>{error.requestId}</code>
              </p>
            ) : null}
            <button type="button" onClick={controller.refresh}>
              Retry delivery status
            </button>
          </div>
        ) : null}

        {projection === null ? (
          error === null ? (
            <p role="status" aria-label="Delivery projection status">
              Loading delivery eligibility…
            </p>
          ) : null
        ) : (
          <>
            <Eligibility
              delivery={projection}
              busy={actionBusy}
              onRunPreflight={confirmation.openPreflight}
            />
            {confirmation.mergeOperation !== null ? (
              <MergeProgress
                operation={confirmation.mergeOperation}
                allowedActions={projection.allowed_actions}
                busy={actionBusy}
                onRequestMerge={confirmation.openMerge}
                onRunPreflightAgain={confirmation.openPreflight}
              />
            ) : null}
            <CleanupControls
              api={api}
              controller={controller}
              taskId={taskId}
              projection={projection}
              operation={cleanupOperation}
              removeRunner={removeRunner}
              deleteRunner={deleteRunner}
              panelFallbackRef={panelRef}
            />
          </>
        )}
      </section>

      {confirmation.dialog !== null ? (
        <PreflightModal
          snapshot={confirmation.dialog}
          operation={confirmation.dialogOperation}
          fresh={confirmation.fresh}
          canSubmit={
            confirmation.dialog.kind === "preflight"
              ? confirmation.canRunPreflight
              : confirmation.canMerge
          }
          runner={
            confirmation.dialog.kind === "preflight"
              ? preflightRunner
              : mergeRunner
          }
          onSubmit={
            confirmation.dialog.kind === "preflight"
              ? confirmation.runPreflight
              : confirmation.runMerge
          }
          onRetry={
            confirmation.dialog.kind === "preflight"
              ? confirmation.runPreflight
              : confirmation.runMerge
          }
          onClose={confirmation.close}
        />
      ) : null}
    </>
  );
}
