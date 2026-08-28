import type { DeliveryCommandRunner } from "./useDeliveryCommandRunner";
import { AccessibleDeliveryDialog } from "./AccessibleDeliveryDialog";
import {
  cleanupKindLabel,
  type CleanupSnapshot,
} from "./cleanupModel";

export interface CleanupDialogProps {
  snapshot: CleanupSnapshot;
  fresh: boolean;
  canSubmit: boolean;
  runner: DeliveryCommandRunner;
  onExecute(): void;
  onClose(): void;
}

export function CleanupDialog({
  snapshot,
  fresh,
  canSubmit,
  runner,
  onExecute,
  onClose,
}: CleanupDialogProps) {
  const deleting = snapshot.kind === "delete_branch";
  const pending = runner.state.phase === "pending";
  const succeeded = runner.state.phase === "succeeded";
  const retryable =
    runner.state.phase === "error" && runner.state.error.retryable;
  const label = (() => {
    if (pending) return deleting ? "Deleting source branch…" : "Removing worktree…";
    if (succeeded) {
      return deleting ? "Branch cleanup accepted" : "Worktree cleanup accepted";
    }
    if (runner.state.phase === "error") {
      return deleting ? "Retry branch cleanup" : "Retry worktree cleanup";
    }
    return deleting ? "Delete exact local branch" : "Remove exact local worktree";
  })();

  return (
    <AccessibleDeliveryDialog
      title={deleting ? "Delete local source branch?" : "Remove local worktree?"}
      description={
        deleting
          ? "This is a separate local cleanup request. It never deletes a remote branch."
          : "This removes only the managed local worktree after the service proves safe unlock and identity conditions. The source branch is retained."
      }
      className="delivery-cleanup-dialog"
      busy={pending}
      closeLabel="Cancel"
      onClose={onClose}
      actions={
        <button
          type="button"
          className="delivery-destructive-action"
          disabled={
            !canSubmit ||
            pending ||
            succeeded ||
            (runner.state.phase === "error" && !retryable)
          }
          onClick={onExecute}
        >
          {label}
        </button>
      }
    >
      {!fresh ? (
        <p className="delivery-stale" role="alert">
          This cleanup confirmation is stale. Close it and review the latest server
          projection.
        </p>
      ) : null}
      {fresh && !canSubmit && !pending && !succeeded ? (
        <p className="delivery-stale" role="alert">
          The server state no longer allows this cleanup confirmation. Close it and
          review the latest projection.
        </p>
      ) : null}
      <dl className="delivery-exact-fields">
        <div>
          <dt>Disposition version</dt>
          <dd>{snapshot.dispositionVersion}</dd>
        </div>
        <div>
          <dt>Merged operation</dt>
          <dd className="delivery-value">
            <code>{snapshot.mergedOperationId}</code>
          </dd>
        </div>
        <div>
          <dt>Source reference</dt>
          <dd className="delivery-value">
            <code>{snapshot.sourceRef}</code>
          </dd>
        </div>
        <div>
          <dt>Source object</dt>
          <dd className="delivery-value">
            <code>{snapshot.sourceOid}</code>
          </dd>
        </div>
        {deleting ? (
          <>
            <div>
              <dt>Verified target branch</dt>
              <dd className="delivery-value">
                <code>{snapshot.targetBranch}</code>
              </dd>
            </div>
            <div>
              <dt>Verified target HEAD</dt>
              <dd className="delivery-value">
                <code>{snapshot.targetHead}</code>
              </dd>
            </div>
          </>
        ) : null}
      </dl>
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
        <p role="status" aria-label={`${cleanupKindLabel(snapshot.kind)} receipt`}>
          Durable receipt: {runner.state.response.receipt}
        </p>
      ) : null}
    </AccessibleDeliveryDialog>
  );
}
