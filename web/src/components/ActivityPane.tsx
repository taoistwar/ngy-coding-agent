import type { ActivityEntry } from "../api/types";

export interface ActivityPaneProps {
  activity: ActivityEntry[];
}

const ACTIVITY_GLYPH: Record<ActivityEntry["level"], string> = {
  info: "i",
  warning: "!",
  error: "×",
};

function actorLabel(entry: ActivityEntry): string {
  switch (entry.actor) {
    case "system":
      return "System";
    case "planner":
      return `Planner #${entry.role_run}`;
    case "executor":
      return `Executor #${entry.role_run}`;
    case "reviewer":
      return `Reviewer #${entry.role_run}`;
  }
}

export function ActivityPane({ activity }: ActivityPaneProps) {
  return (
    <section className="evidence-panel activity-panel" aria-labelledby="activity-heading">
      <h3 id="activity-heading">Activity</h3>
      <div
        className="activity-log"
        role="log"
        aria-label="Task activity"
        aria-live="polite"
        aria-relevant="additions"
      >
        {activity.length === 0 ? (
          <p className="empty-state">No activity yet.</p>
        ) : (
          <ol>
            {activity.map((entry) => (
              <li key={entry.id} className={`activity-entry level-${entry.level}`}>
                <span className="status-glyph" aria-hidden="true">
                  {ACTIVITY_GLYPH[entry.level]}
                </span>
                <span>
                  <span className="activity-meta">
                    <strong>{actorLabel(entry)}</strong>
                    <span>{entry.level}</span>
                  </span>
                  <span className="activity-message">{entry.message}</span>
                  <time dateTime={entry.created_at}>{entry.created_at}</time>
                </span>
              </li>
            ))}
          </ol>
        )}
      </div>
    </section>
  );
}
