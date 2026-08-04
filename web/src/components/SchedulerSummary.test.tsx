import { cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { SchedulerQueueReason, SchedulerState } from "../api/types";
import type { SchedulerProjectionState } from "../state/schedulerProjection";
import {
  SchedulerSummary,
  schedulerQueueReasonLabel,
} from "./SchedulerSummary";

afterEach(cleanup);

const REASONS: SchedulerQueueReason[] = [
  "service_paused",
  "storage_pressure",
  "global_capacity",
  "repository_capacity",
  "repository_control_busy",
];

function snapshot(): SchedulerState {
  return {
    schema_version: 1,
    server_instance_id: "123e4567-e89b-42d3-a456-426614174000",
    server_started_at: "2026-07-15T00:00:00Z",
    generation: 7,
    as_of_event_id: 23,
    service_state_generation: 4,
    admission_state: "running",
    limits: {
      global: 4,
      per_repository: 2,
      queued: 3,
      cargo_jobs_per_task: 8,
    },
    active_task_count: 3,
    queued_task_count: 5,
    queued_tasks: REASONS.map((reason, index) => ({
      task_id: `123e4567-e89b-42d3-a456-42661417400${index}`,
      reason,
    })),
    stopping_tasks: [],
    storage: {
      state: "normal",
      data: { state: "normal" },
      runtime: { state: "normal" },
      repositories: [],
    },
  };
}

function projection(
  freshness: SchedulerProjectionState["freshness"],
): SchedulerProjectionState {
  return {
    snapshot: snapshot(),
    freshness,
    staleReason: freshness === "stale" ? "transport_reconnecting" : null,
    digest: "a".repeat(64),
    canonicalJson: "{}",
    pending: null,
    recoveryReason: null,
  };
}

describe("SchedulerSummary", () => {
  it("shows exact capacity, legacy over-limit queue usage, and only fixed queue reason copy", () => {
    render(<SchedulerSummary scheduler={projection("fresh")} />);

    const summary = screen.getByRole("region", { name: "Controlled concurrency" });
    expect(within(summary).getByText("3 / 4 active")).toBeVisible();
    expect(within(summary).getByText("2 per repository")).toBeVisible();
    expect(within(summary).getByText("5 / 3 queued")).toBeVisible();
    expect(within(summary).getByText("8 Cargo jobs per task")).toBeVisible();
    expect(summary).not.toHaveTextContent(/position|ETA/iu);
    expect(within(summary).queryByRole("button")).not.toBeInTheDocument();
  });

  it.each([
    ["service_paused", "Waiting for the service"],
    ["storage_pressure", "Waiting for storage"],
    ["global_capacity", "Waiting for global capacity"],
    ["repository_capacity", "Waiting for repository capacity"],
    ["repository_control_busy", "Waiting for repository coordination"],
  ] as const)("maps %s to fixed server reason copy", (reason, label) => {
    expect(schedulerQueueReasonLabel(reason)).toBe(label);
  });

  it("retains the last snapshot while explicitly marking it stale", () => {
    render(<SchedulerSummary scheduler={projection("stale")} />);

    expect(screen.getByText("Last known scheduler state — stale")).toBeVisible();
    expect(screen.getByText("3 / 4 active")).toBeVisible();
  });

  it("reports unavailable state without inventing capacity", () => {
    render(
      <SchedulerSummary
        scheduler={{
          snapshot: null,
          freshness: "unavailable",
          staleReason: null,
          digest: null,
          canonicalJson: null,
          pending: null,
          recoveryReason: null,
        }}
      />,
    );

    expect(screen.getByText("Scheduler state is unavailable")).toBeVisible();
    expect(screen.queryByText(/active/iu)).not.toBeInTheDocument();
  });
});
