import type { PlanSnapshot } from "../api/types";

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
      {plan === null || plan.items.length === 0 ? (
        <p className="empty-state">No plan has been published yet.</p>
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
                <strong>{item.title}</strong>
                <span>{PLAN_STATUS[item.status]}</span>
              </span>
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
