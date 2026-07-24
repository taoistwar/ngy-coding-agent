import type { PlanSnapshot } from "../api/types";
import { RequiredChecks } from "./RequiredChecks";

export interface PlanPaneProps {
  plan: PlanSnapshot | null;
}

const PLAN_STATUS: Record<PlanSnapshot["items"][number]["status"], string> = {
  pending: "Pending",
  running: "In progress",
  completed: "Completed",
};

const PLAN_GLYPH: Record<PlanSnapshot["items"][number]["status"], string> = {
  pending: "○",
  running: "◐",
  completed: "✓",
};

export function PlanPane({ plan }: PlanPaneProps) {
  return (
    <section className="evidence-panel plan-panel" aria-labelledby="plan-heading">
      <div className="panel-heading-row">
        <h3 id="plan-heading">Plan</h3>
        {plan !== null ? <span>Revision {plan.revision}</span> : null}
      </div>
      {plan === null ? (
        <p className="empty-state">No plan has been published yet.</p>
      ) : (
        <>
          {plan.format_version === 0 ? (
            <p className="legacy-plan-note">
              Legacy plan: structured summary and acceptance criteria were not
              recorded.
            </p>
          ) : (
            <div className="plan-summary">
              <strong>Planner summary</strong>
              <p>{plan.summary}</p>
            </div>
          )}
          {plan.items.length === 0 ? (
            <p className="empty-state">No plan steps were recorded.</p>
          ) : (
            <ol className="plan-list">
              {plan.items.map((item, index) => (
                <li
                  key={item.id}
                  className={`plan-step status-${item.status}`}
                  aria-label={`Plan step ${index + 1}: ${item.title}, ${PLAN_STATUS[item.status]}`}
                >
                  <span className="status-glyph" aria-hidden="true">
                    {PLAN_GLYPH[item.status]}
                  </span>
                  <span className="plan-step-copy">
                    <span className="plan-step-heading">
                      <strong>{item.title}</strong>
                      <span>{PLAN_STATUS[item.status]}</span>
                    </span>
                    {plan.format_version === 1 ? (
                      <>
                        <span className="plan-description">
                          {item.description}
                        </span>
                        <span className="plan-criteria-label">
                          Acceptance criteria
                        </span>
                        <ul className="plan-criteria">
                          {item.acceptance_criteria.map((criterion) => (
                            <li key={criterion}>{criterion}</li>
                          ))}
                        </ul>
                      </>
                    ) : null}
                  </span>
                </li>
              ))}
            </ol>
          )}
          {plan.format_version === 1 ? (
            <div className="plan-checks">
              <h4>Initial required checks</h4>
              <RequiredChecks
                checks={plan.initial_required_checks}
                emptyMessage="No initial required checks were recorded."
              />
            </div>
          ) : null}
        </>
      )}
    </section>
  );
}
