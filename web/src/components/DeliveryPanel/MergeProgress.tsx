import { useId } from "react";

import type {
  DeliveryAllowedAction,
  DeliveryConflictSummary,
  DeliveryMergeOperation,
  DeliveryMergeState,
} from "../../api/types";

export interface MergeProgressProps {
  operation: DeliveryMergeOperation;
  allowedActions: readonly DeliveryAllowedAction[];
  busy?: boolean;
  onRequestMerge: (trigger: HTMLElement) => void;
  onRunPreflightAgain: (trigger: HTMLElement) => void;
}

export interface ConflictPathsProps {
  conflicts: DeliveryConflictSummary | null;
}

const MERGE_STATE_LABEL: Record<DeliveryMergeState, string> = {
  preflight_pending: "Preflight in progress",
  preflight_ready: "Preflight ready",
  accepted: "Local merge accepted",
  merge_pending: "Local merge in progress",
  merged: "Merged locally",
  abort_pending: "Local merge abort in progress",
  conflict: "Preflight found conflicts",
  rejected: "Preflight rejected",
  stale: "Preflight stale",
  superseded: "Preflight superseded",
  failed: "Local merge failed",
  reconciliation_required: "Manual reconciliation required",
};

const MERGE_STATE_DESCRIPTION: Record<DeliveryMergeState, string> = {
  preflight_pending:
    "Preflight is running. No target branch change has been authorized.",
  preflight_ready:
    "Preflight completed without conflicts. Review the exact operation before confirming the local merge.",
  accepted: "The local merge was accepted and is waiting to run.",
  merge_pending: "The local merge is in progress.",
  merged:
    "Merged into the local target branch. The source worktree and branch are retained until you remove them explicitly.",
  abort_pending: "A conflicting local merge is being safely aborted.",
  conflict:
    "Preflight found conflicts. Resolve them outside this panel, then run preflight again when the server allows it.",
  rejected:
    "Preflight was rejected. This preflight did not change the target branch.",
  stale:
    "The preflight is stale. Run a new preflight when the server allows it.",
  superseded: "This preflight was replaced by a newer preflight.",
  failed: "The local merge operation failed.",
  reconciliation_required:
    "The local repository state needs manual reconciliation. No action is available here.",
};

const PENDING_STATES = new Set<DeliveryMergeState>([
  "preflight_pending",
  "accepted",
  "merge_pending",
  "abort_pending",
]);

function fingerprintSummary(value: string): string {
  return value.length <= 16 ? value : `${value.slice(0, 12)}…`;
}

export function ConflictPaths({ conflicts }: ConflictPathsProps) {
  const headingId = useId();
  if (conflicts === null) {
    return <p className="delivery-empty-state">Conflict details are unavailable.</p>;
  }

  return (
    <section className="delivery-conflicts" aria-labelledby={headingId}>
      <h5 id={headingId}>Conflict paths</h5>
      <p>
        Showing {conflicts.paths.length} of {conflicts.path_count} conflict
        {conflicts.path_count === 1 ? " path" : " paths"}.
      </p>
      {conflicts.paths.length === 0 ? null : (
        <ul aria-label="Bounded relative conflict paths">
          {conflicts.paths.map((conflictPath, index) => (
            <li key={`${conflictPath.encoding}:${conflictPath.path}:${index}`}>
              {conflictPath.encoding === "base64url" ? (
                <span className="delivery-path-encoding">base64url encoded</span>
              ) : null}
              <code className="delivery-long-value delivery-conflict-path">
                {conflictPath.path}
              </code>
            </li>
          ))}
        </ul>
      )}
      <p className="delivery-conflict-bounds">
        {conflicts.truncated
          ? "The server truncated this bounded path summary."
          : "The server returned the complete bounded path summary."}
      </p>
    </section>
  );
}

export function MergeProgress({
  operation,
  allowedActions,
  busy = false,
  onRequestMerge,
  onRunPreflightAgain,
}: MergeProgressProps) {
  const pending = PENDING_STATES.has(operation.state);
  const canRequestMerge =
    operation.state === "preflight_ready" &&
    allowedActions.includes("accept_merge");
  const canRunPreflightAgain =
    operation.state === "conflict" &&
    allowedActions.includes("run_preflight");

  return (
    <section
      className={`delivery-merge-progress delivery-merge-${operation.state}`}
      aria-labelledby="delivery-merge-progress-heading"
      aria-busy={busy || pending}
    >
      <h4 id="delivery-merge-progress-heading">Merge progress</h4>
      <p className="delivery-status" role="status" aria-live="polite">
        <strong>{MERGE_STATE_LABEL[operation.state]}</strong>.{" "}
        {MERGE_STATE_DESCRIPTION[operation.state]}
      </p>

      <dl className="delivery-facts">
        <div>
          <dt>Operation ID</dt>
          <dd>
            <code className="delivery-long-value">{operation.operation_id}</code>
          </dd>
        </div>
        <div>
          <dt>Operation version</dt>
          <dd>{operation.version}</dd>
        </div>
        <div>
          <dt>Target branch</dt>
          <dd>
            <code className="delivery-long-value">{operation.target_branch}</code>
          </dd>
        </div>
        <div>
          <dt>Target HEAD</dt>
          <dd>
            <code className="delivery-long-value">{operation.target_head}</code>
          </dd>
        </div>
        <div>
          <dt>Review generation</dt>
          <dd>{operation.review_generation}</dd>
        </div>
        <div>
          <dt>Workspace fingerprint</dt>
          <dd>
            <code
              className="delivery-long-value delivery-fingerprint-summary"
              aria-label={`Workspace fingerprint summary ${fingerprintSummary(operation.workspace_fingerprint)}`}
            >
              {fingerprintSummary(operation.workspace_fingerprint)}
            </code>
          </dd>
        </div>
        {operation.source_commit === null ? null : (
          <div>
            <dt>Local source commit</dt>
            <dd>
              <code className="delivery-long-value">{operation.source_commit}</code>
            </dd>
          </div>
        )}
      </dl>

      {operation.failure === null ? null : (
        <p className="delivery-failure-code">
          Failure code: {" "}
          <code className="delivery-long-value">{operation.failure.code}</code>
        </p>
      )}

      {operation.state === "conflict" ? (
        <ConflictPaths conflicts={operation.conflicts} />
      ) : null}

      {pending ? (
        <button type="button" disabled aria-label={MERGE_STATE_LABEL[operation.state]}>
          {MERGE_STATE_LABEL[operation.state]}
        </button>
      ) : null}

      {canRequestMerge ? (
        <button
          type="button"
          onClick={(event) => onRequestMerge(event.currentTarget)}
          disabled={busy}
          aria-label="Review and confirm local merge"
        >
          {busy ? "Opening confirmation…" : "Review and confirm local merge"}
        </button>
      ) : null}

      {canRunPreflightAgain ? (
        <button
          type="button"
          onClick={(event) => onRunPreflightAgain(event.currentTarget)}
          disabled={busy}
          aria-label="Run delivery preflight again"
        >
          {busy ? "Running preflight…" : "Run preflight again"}
        </button>
      ) : null}
    </section>
  );
}
