import type { DeliveryTask } from "../../api/types";

export interface EligibilityProps {
  delivery: DeliveryTask;
  busy?: boolean;
  onRunPreflight: (trigger: HTMLElement) => void;
}

type EligibilityReason = DeliveryTask["reasons"][number];

const ELIGIBILITY_REASON_LABEL: Record<EligibilityReason, string> = {
  task_not_completed: "The task has not completed.",
  review_not_approved: "The final review is not approved.",
  approved_evidence_missing: "Approved review evidence is unavailable.",
  attempt_artifact_missing: "The task attempt artifact is unavailable.",
  attempt_artifact_not_ready: "The task attempt artifact is not ready.",
  task_active: "The task is still active.",
  process_cleanup_unproven: "Process cleanup has not been proven.",
  target_branch_detached: "The target worktree is detached.",
  target_branch_mismatch: "The observed target branch does not match.",
  target_head_changed: "The target HEAD changed.",
  target_worktree_dirty: "The target worktree has local changes.",
  target_ignored_path_collision:
    "An ignored target path would collide with the delivery.",
  target_git_operation_in_progress:
    "Another Git operation is in progress in the target worktree.",
  unsafe_git_configuration: "The repository Git configuration is unsafe.",
  unsupported_git_attributes:
    "The repository uses unsupported Git attributes for delivery.",
  source_already_in_target: "The delivery source is already in the target.",
  runtime_drift: "The current runtime observation no longer matches.",
  delivery_owned: "Another delivery operation currently owns this task.",
  already_merged: "This task has already been merged locally.",
  reconciliation_required: "The repository needs manual reconciliation.",
  repository_busy: "The repository is busy.",
  repository_unavailable: "The repository is unavailable.",
  store_unavailable: "Delivery state is unavailable.",
  runtime_observation_unavailable:
    "The current runtime observation is unavailable.",
  service_not_ready: "The delivery service is not ready.",
};

const ELIGIBILITY_LABEL: Record<DeliveryTask["eligibility"], string> = {
  eligible: "Eligible for local delivery",
  ineligible: "Not eligible for local delivery",
  unavailable: "Delivery eligibility unavailable",
};

export function deliveryEligibilityReasonLabel(
  reason: EligibilityReason,
): string {
  return ELIGIBILITY_REASON_LABEL[reason];
}

function fingerprintSummary(value: string): string {
  return value.length <= 16 ? value : `${value.slice(0, 12)}…`;
}

export function Eligibility({
  delivery,
  busy = false,
  onRunPreflight,
}: EligibilityProps) {
  const target = delivery.target;
  const availableTarget =
    target.available === true && "branch" in target ? target : null;
  const canRunPreflight =
    delivery.eligibility === "eligible" &&
    delivery.allowed_actions.includes("run_preflight") &&
    delivery.latest_merge?.state !== "conflict";

  return (
    <section
      className={`delivery-eligibility delivery-eligibility-${delivery.eligibility}`}
      aria-labelledby="delivery-eligibility-heading"
      aria-busy={busy}
    >
      <h4 id="delivery-eligibility-heading">Delivery eligibility</h4>
      <p className="delivery-status" role="status" aria-live="polite">
        {ELIGIBILITY_LABEL[delivery.eligibility]}
      </p>

      {delivery.eligibility === "eligible" &&
      availableTarget !== null &&
      delivery.evidence !== null ? (
        <dl className="delivery-facts">
          <div>
            <dt>Target branch</dt>
            <dd>
              <code className="delivery-long-value">{availableTarget.branch}</code>
            </dd>
          </div>
          <div>
            <dt>Target HEAD</dt>
            <dd>
              <code className="delivery-long-value">{availableTarget.head}</code>
            </dd>
          </div>
          <div>
            <dt>Review generation</dt>
            <dd>{delivery.evidence.review_generation}</dd>
          </div>
          <div>
            <dt>Workspace fingerprint</dt>
            <dd>
              <code
                className="delivery-long-value delivery-fingerprint-summary"
                aria-label={`Workspace fingerprint summary ${fingerprintSummary(delivery.evidence.workspace_fingerprint)}`}
              >
                {fingerprintSummary(delivery.evidence.workspace_fingerprint)}
              </code>
            </dd>
          </div>
        </dl>
      ) : (
        <ul className="delivery-reasons" aria-label="Delivery eligibility reasons">
          {delivery.reasons.map((reason) => (
            <li key={reason}>{deliveryEligibilityReasonLabel(reason)}</li>
          ))}
        </ul>
      )}

      {canRunPreflight ? (
        <button
          type="button"
          onClick={(event) => onRunPreflight(event.currentTarget)}
          disabled={busy}
          aria-label={busy ? "Running delivery preflight" : "Run delivery preflight"}
        >
          {busy ? "Running preflight…" : "Run preflight"}
        </button>
      ) : null}
    </section>
  );
}
