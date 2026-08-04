import type {
  SchedulerQueueReason,
  SchedulerStopIntent,
} from "../api/types";
import type { SchedulerProjectionState } from "../state/schedulerProjection";

export interface SchedulerSummaryProps {
  scheduler: SchedulerProjectionState;
}

const QUEUE_REASON_LABEL: Record<SchedulerQueueReason, string> = {
  service_paused: "Waiting for the service",
  storage_pressure: "Waiting for storage",
  global_capacity: "Waiting for global capacity",
  repository_capacity: "Waiting for repository capacity",
  repository_control_busy: "Waiting for repository coordination",
};

const STOP_INTENT_LABEL: Record<SchedulerStopIntent, string> = {
  user_cancelled: "Stopping — user requested",
  disk_pressure_critical: "Stopping — critical storage pressure",
};

export function schedulerQueueReasonLabel(reason: SchedulerQueueReason): string {
  return QUEUE_REASON_LABEL[reason];
}

export function schedulerStopIntentLabel(intent: SchedulerStopIntent): string {
  return STOP_INTENT_LABEL[intent];
}

export function SchedulerSummary({ scheduler }: SchedulerSummaryProps) {
  const snapshot = scheduler.snapshot;
  return (
    <section
      className={`scheduler-summary scheduler-${scheduler.freshness}`}
      aria-label="Controlled concurrency"
    >
      <div className="scheduler-summary-heading">
        <div>
          <p className="eyebrow">Controlled concurrency and admission</p>
          <h2 id="scheduler-summary-heading">Scheduler</h2>
        </div>
        <p className="scheduler-freshness">
          {scheduler.freshness === "fresh"
            ? "Scheduler state is current"
            : scheduler.freshness === "stale"
              ? "Last known scheduler state — stale"
              : "Scheduler state is unavailable"}
        </p>
      </div>

      {snapshot === null ? null : (
        <>
          {scheduler.freshness === "stale" ? (
            <p className="scheduler-stale-note">
              This snapshot is retained for reference and is not a current admission fact.
            </p>
          ) : null}
          <dl className="scheduler-capacity">
            <div>
              <dt>Active tasks</dt>
              <dd>
                {snapshot.active_task_count} / {snapshot.limits.global} active
              </dd>
            </div>
            <div>
              <dt>Repository limit</dt>
              <dd>{snapshot.limits.per_repository} per repository</dd>
            </div>
            <div>
              <dt>Queue usage</dt>
              <dd>
                {snapshot.queued_task_count} / {snapshot.limits.queued} queued
              </dd>
            </div>
            <div>
              <dt>Cargo parallelism</dt>
              <dd>{snapshot.limits.cargo_jobs_per_task} Cargo jobs per task</dd>
            </div>
          </dl>

        </>
      )}
    </section>
  );
}
