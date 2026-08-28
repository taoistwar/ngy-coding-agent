import type {
  DeliveryMergeOperationEnvelope,
  DeliveryTask,
} from "../../api/types";
import type { DeliveryModalAuthority } from "../../state/deliveryModel";
import { AccessibleDeliveryDialog } from "./AccessibleDeliveryDialog";
import type { DeliveryCommandRunner } from "./useDeliveryCommandRunner";
import { ConflictPaths } from "./MergeProgress";

export {
  AccessibleDeliveryDialog,
  restoreDeliveryDialogFocus,
  type AccessibleDeliveryDialogProps,
} from "./AccessibleDeliveryDialog";

export interface DeliveryConfirmationSnapshot {
  kind: "preflight" | "merge";
  taskId: string;
  operationId: string | null;
  operationVersion: number | null;
  authority: DeliveryModalAuthority;
  sourceState: NonNullable<DeliveryTask["source"]>["state"] | null;
  sourceRef: string | null;
  sourceOid: string | null;
  operation: DeliveryMergeOperationEnvelope | null;
  scopeKey: string;
  returnFocus: HTMLElement | null;
}

export interface PreflightModalProps {
  snapshot: DeliveryConfirmationSnapshot;
  operation: DeliveryMergeOperationEnvelope | null;
  fresh: boolean;
  canSubmit: boolean;
  runner: DeliveryCommandRunner;
  onSubmit(): void;
  onRetry(): void;
  onClose(): void;
}

export function PreflightModal({
  snapshot,
  operation,
  fresh,
  canSubmit,
  runner,
  onSubmit,
  onRetry,
  onClose,
}: PreflightModalProps) {
  const mergeConfirmation = snapshot.kind === "merge";
  const pending = runner.state.phase === "pending";
  const succeeded = runner.state.phase === "succeeded";
  const retryable =
    runner.state.phase === "error" && runner.state.error.retryable;
  const enabled = fresh && canSubmit && !pending && !succeeded;
  const primaryLabel = (() => {
    if (pending) {
      return mergeConfirmation ? "Accepting merge…" : "Running preflight…";
    }
    if (succeeded) {
      return mergeConfirmation ? "Merge accepted" : "Preflight accepted";
    }
    if (runner.state.phase === "error") {
      return mergeConfirmation ? "Retry merge request" : "Retry preflight";
    }
    return mergeConfirmation ? "Merge locally" : "Run preflight";
  })();

  return (
    <AccessibleDeliveryDialog
      title={
        mergeConfirmation
          ? "Confirm exact local merge"
          : "Confirm local merge preflight"
      }
      description={
        mergeConfirmation
          ? "Confirm the exact ready operation below. A changed version or authority tuple makes this confirmation stale."
          : "Preflight checks the exact approved evidence and local target below without modifying the target branch."
      }
      busy={pending}
      closeLabel={mergeConfirmation ? "Cancel" : "Close"}
      onClose={onClose}
      actions={
        <button
          type="button"
          className={mergeConfirmation ? "delivery-primary-action" : undefined}
          disabled={!enabled || (runner.state.phase === "error" && !retryable)}
          onClick={runner.state.phase === "error" ? onRetry : onSubmit}
        >
          {primaryLabel}
        </button>
      }
    >
      {!fresh ? (
        <p className="delivery-stale" role="alert">
          This confirmation is stale. Close it and review the latest server state.
        </p>
      ) : null}

      <dl className="delivery-exact-fields">
        <div>
          <dt>Review generation</dt>
          <dd className="delivery-value">{snapshot.authority.reviewGeneration}</dd>
        </div>
        <div>
          <dt>Workspace fingerprint</dt>
          <dd className="delivery-value">
            <code>{snapshot.authority.workspaceFingerprint}</code>
          </dd>
        </div>
        <div>
          <dt>Source state</dt>
          <dd>{snapshot.sourceState ?? "Not materialized"}</dd>
        </div>
        <div>
          <dt>Source reference</dt>
          <dd className="delivery-value">
            {snapshot.sourceRef === null ? "Not materialized" : <code>{snapshot.sourceRef}</code>}
          </dd>
        </div>
        <div>
          <dt>Source object</dt>
          <dd className="delivery-value">
            {snapshot.sourceOid === null ? "Not materialized" : <code>{snapshot.sourceOid}</code>}
          </dd>
        </div>
        <div>
          <dt>Target branch</dt>
          <dd className="delivery-value">
            <code>{snapshot.authority.targetBranch}</code>
          </dd>
        </div>
        <div>
          <dt>Target HEAD</dt>
          <dd className="delivery-value">
            <code>{snapshot.authority.targetHead}</code>
          </dd>
        </div>
        {snapshot.operationId !== null ? (
          <>
            <div>
              <dt>Operation ID</dt>
              <dd className="delivery-value">
                <code>{snapshot.operationId}</code>
              </dd>
            </div>
            <div>
              <dt>Confirmed operation version</dt>
              <dd>{snapshot.operationVersion}</dd>
            </div>
          </>
        ) : null}
        {operation?.candidate_source_tree !== null &&
        operation?.candidate_source_tree !== undefined ? (
          <div>
            <dt>Candidate source tree</dt>
            <dd className="delivery-value">
              <code>{operation.candidate_source_tree}</code>
            </dd>
          </div>
        ) : null}
        {operation?.preflight_source_commit !== null &&
        operation?.preflight_source_commit !== undefined ? (
          <div>
            <dt>Preflight source commit</dt>
            <dd className="delivery-value">
              <code>{operation.preflight_source_commit}</code>
            </dd>
          </div>
        ) : null}
      </dl>

      <PreflightResult operation={operation} />

      {runner.state.phase !== "idle" ? (
        <p className="delivery-request-id">
          Client request ID: <code>{runner.state.clientRequestId}</code>
        </p>
      ) : null}
      {runner.state.phase === "error" ? (
        <div className="delivery-command-error" role="alert">
          <p>{runner.state.error.message}</p>
          <p>
            Error code: <code>{runner.state.error.code}</code>
          </p>
          {runner.state.error.requestId !== null ? (
            <p>
              Server request ID: <code>{runner.state.error.requestId}</code>
            </p>
          ) : null}
        </div>
      ) : null}
      {runner.state.phase === "succeeded" ? (
        <p role="status" aria-label="Delivery command receipt">
          Durable receipt: {runner.state.response.receipt}
        </p>
      ) : null}
    </AccessibleDeliveryDialog>
  );
}

function PreflightResult({
  operation,
}: {
  operation: DeliveryMergeOperationEnvelope | null;
}) {
  if (operation === null) return null;
  if (operation.state === "preflight_pending") {
    return (
      <p role="status" aria-label="Preflight result">
        Preflight is pending for operation version {operation.version}.
      </p>
    );
  }
  if (operation.state === "preflight_ready") {
    return (
      <p className="delivery-success" role="status" aria-label="Preflight result">
        Preflight is clean. Merge requires a separate confirmation of this exact
        ready version.
      </p>
    );
  }
  if (operation.state === "conflict" || operation.state === "abort_pending") {
    return operation.conflicts === null ? null : (
      <div className="delivery-conflict-result">
        <p role="status" aria-label="Preflight result">
          Preflight found a merge conflict. No files are edited automatically.
        </p>
        <ConflictPaths conflicts={operation.conflicts} />
      </div>
    );
  }
  return (
    <p role="status" aria-label="Preflight result">
      Current operation state: {operation.state}
    </p>
  );
}
