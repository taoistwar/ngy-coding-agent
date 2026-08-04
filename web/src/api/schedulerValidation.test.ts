import { describe, expect, it } from "vitest";

import type { BootstrapResponse, Repository, Task } from "./types";
import {
  aggregateStorageState,
  validateSchedulerStateAgainstAuthority,
} from "./schedulerValidation";
import { validateBootstrapResponse } from "./validation";

const STARTED_AT = "2026-07-29T01:00:00Z";
const REPOSITORY_A = "00000000-0000-4000-8000-000000000001";
const REPOSITORY_B = "00000000-0000-4000-8000-000000000002";
const RUNNING_TASK = "00000000-0000-4000-8000-000000000010";
const EARLY_QUEUED_TASK = "00000000-0000-4000-8000-000000000011";
const LATE_QUEUED_TASK = "00000000-0000-4000-8000-000000000012";
const NON_RUNNING_TASK = "00000000-0000-4000-8000-000000000013";

function repository(id: string): Repository {
  return {
    id,
    selected_path: `E:\\${id}`,
    display_name: id,
    git_root: `E:\\${id}`,
    cargo_workspace_root: `E:\\${id}`,
    created_at: STARTED_AT,
    last_opened_at: STARTED_AT,
  };
}

function task(
  id: string,
  repositoryId: string,
  status: "queued" | "running",
  createdAt: string,
  eventId: number,
): Task {
  return {
    id,
    client_request_id: id,
    repository_id: repositoryId,
    prompt: `work ${id}`,
    status,
    delivery_readiness: "unreviewed",
    attempt: 1,
    retry_of: null,
    created_at: createdAt,
    started_at: status === "running" ? STARTED_AT : null,
    finished_at: null,
    last_event_id: eventId,
    failure: null,
  };
}

function validBootstrap(): BootstrapResponse {
  return {
    csrf_token: "csrf",
    repositories: [repository(REPOSITORY_B), repository(REPOSITORY_A)],
    tasks: [
      task(
        LATE_QUEUED_TASK,
        REPOSITORY_B,
        "queued",
        "2026-07-29T01:00:01.1Z",
        13,
      ),
      task(
        RUNNING_TASK,
        REPOSITORY_A,
        "running",
        "2026-07-29T01:00:00Z",
        11,
      ),
      task(
        EARLY_QUEUED_TASK,
        REPOSITORY_A,
        "queued",
        "2026-07-29T01:00:01.02Z",
        12,
      ),
    ],
    latest_event_id: 15,
    server_started_at: STARTED_AT,
    service_state: "ready",
    service_state_generation: 7,
    max_concurrent_tasks: 2,
    scheduler: {
      schema_version: 1,
      server_instance_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      server_started_at: STARTED_AT,
      generation: 8,
      as_of_event_id: 14,
      service_state_generation: 7,
      admission_state: "running",
      limits: {
        global: 2,
        per_repository: 1,
        queued: 32,
        cargo_jobs_per_task: 4,
      },
      active_task_count: 1,
      queued_task_count: 2,
      queued_tasks: [
        {
          task_id: EARLY_QUEUED_TASK,
          reason: "repository_capacity",
        },
        {
          task_id: LATE_QUEUED_TASK,
          reason: "global_capacity",
        },
      ],
      stopping_tasks: [
        {
          task_id: RUNNING_TASK,
          intent: "user_cancelled",
        },
      ],
      storage: {
        state: "normal",
        data: { state: "normal" },
        runtime: { state: "normal" },
        repositories: [
          { repository_id: REPOSITORY_A, state: "normal" },
          { repository_id: REPOSITORY_B, state: "normal" },
        ],
      },
    },
  };
}

function retainedTerminalPermitBootstrap(): BootstrapResponse {
  const bootstrap = validBootstrap();
  return {
    ...bootstrap,
    tasks: bootstrap.tasks.map((candidate): Task =>
      candidate.id === RUNNING_TASK
        ? {
            ...candidate,
            status: "completed",
            finished_at: "2026-07-29T01:00:02Z",
            last_event_id: 14,
          }
        : candidate,
    ),
    scheduler: {
      ...bootstrap.scheduler,
      active_task_count: 1,
      stopping_tasks: [],
    },
  };
}

describe("scheduler Bootstrap validation", () => {
  it("accepts the exact bounded snapshot and authoritative collection order", () => {
    const bootstrap = validBootstrap();

    expect(validateBootstrapResponse(bootstrap)).toBe(bootstrap);
  });

  it("accepts a terminal task whose permit remains active until safe release", () => {
    const bootstrap = retainedTerminalPermitBootstrap();

    expect(validateBootstrapResponse(bootstrap)).toBe(bootstrap);
  });

  it.each([
    [
      "unknown field",
      (bootstrap: BootstrapResponse) => ({
        ...bootstrap,
        scheduler: { ...bootstrap.scheduler, extra: true },
      }),
    ],
    [
      "missing field",
      (bootstrap: BootstrapResponse) => {
        const { global: _global, ...limits } = bootstrap.scheduler.limits;
        return {
          ...bootstrap,
          scheduler: { ...bootstrap.scheduler, limits },
        };
      },
    ],
    [
      "null object",
      (bootstrap: BootstrapResponse) => ({
        ...bootstrap,
        scheduler: {
          ...bootstrap.scheduler,
          storage: { ...bootstrap.scheduler.storage, data: null },
        },
      }),
    ],
    [
      "unsafe integer",
      (bootstrap: BootstrapResponse) => ({
        ...bootstrap,
        scheduler: {
          ...bootstrap.scheduler,
          generation: Number.MAX_SAFE_INTEGER + 1,
        },
      }),
    ],
    [
      "non-canonical UUID",
      (bootstrap: BootstrapResponse) => ({
        ...bootstrap,
        scheduler: {
          ...bootstrap.scheduler,
          server_instance_id: "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        },
      }),
    ],
    [
      "non-v4 epoch",
      (bootstrap: BootstrapResponse) => ({
        ...bootstrap,
        scheduler: {
          ...bootstrap.scheduler,
          server_instance_id: "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa",
        },
      }),
    ],
    [
      "unknown enum",
      (bootstrap: BootstrapResponse) => ({
        ...bootstrap,
        scheduler: {
          ...bootstrap.scheduler,
          queued_tasks: [
            {
              ...bootstrap.scheduler.queued_tasks[0]!,
              reason: "capacity",
            },
            bootstrap.scheduler.queued_tasks[1]!,
          ],
        },
      }),
    ],
  ] as const)("rejects an exact-object violation: %s", (_name, mutate) => {
    expect(() => validateBootstrapResponse(mutate(validBootstrap()))).toThrow();
  });

  it.each([
    [
      "global alias",
      (bootstrap: BootstrapResponse) => ({
        ...bootstrap,
        max_concurrent_tasks: 1,
      }),
    ],
    [
      "started-at alias",
      (bootstrap: BootstrapResponse) => ({
        ...bootstrap,
        scheduler: {
          ...bootstrap.scheduler,
          server_started_at: "2026-07-29T01:00:01Z",
        },
      }),
    ],
    [
      "service generation alias",
      (bootstrap: BootstrapResponse) => ({
        ...bootstrap,
        scheduler: {
          ...bootstrap.scheduler,
          service_state_generation: 6,
        },
      }),
    ],
    [
      "membership watermark",
      (bootstrap: BootstrapResponse) => ({
        ...bootstrap,
        scheduler: { ...bootstrap.scheduler, as_of_event_id: 16 },
      }),
    ],
    [
      "active count below durable Running count",
      (bootstrap: BootstrapResponse) => ({
        ...bootstrap,
        scheduler: {
          ...bootstrap.scheduler,
          active_task_count: 0,
          stopping_tasks: [],
        },
      }),
    ],
    [
      "queued count",
      (bootstrap: BootstrapResponse) => ({
        ...bootstrap,
        scheduler: { ...bootstrap.scheduler, queued_task_count: 1 },
      }),
    ],
  ] as const)("rejects a cross-constraint mismatch: %s", (_name, mutate) => {
    expect(() => validateBootstrapResponse(mutate(validBootstrap()))).toThrow();
  });

  it.each([
    ["above limits.global", 3],
    ["outside the JSON safe-integer range", Number.MAX_SAFE_INTEGER + 1],
  ] as const)("rejects active_task_count %s", (_name, activeTaskCount) => {
    const bootstrap = validBootstrap();
    expect(() =>
      validateBootstrapResponse({
        ...bootstrap,
        scheduler: {
          ...bootstrap.scheduler,
          active_task_count: activeTaskCount,
        },
      }),
    ).toThrow();
  });

  it("rejects queue reordering, missing repository coverage, and invalid stopping references", () => {
    const bootstrap = validBootstrap();
    const reordered = {
      ...bootstrap,
      scheduler: {
        ...bootstrap.scheduler,
        queued_tasks: [...bootstrap.scheduler.queued_tasks].reverse(),
      },
    };
    const missingStorage = {
      ...bootstrap,
      scheduler: {
        ...bootstrap.scheduler,
        storage: {
          ...bootstrap.scheduler.storage,
          repositories: bootstrap.scheduler.storage.repositories.slice(0, 1),
        },
      },
    };
    const nonRunningStop = {
      ...bootstrap,
      scheduler: {
        ...bootstrap.scheduler,
        stopping_tasks: [
          {
            task_id: NON_RUNNING_TASK,
            intent: "disk_pressure_critical",
          },
        ],
      },
    };

    expect(() => validateBootstrapResponse(reordered)).toThrow(
      /authoritative/,
    );
    expect(() => validateBootstrapResponse(missingStorage)).toThrow(
      /exactly cover Bootstrap repositories/,
    );
    expect(() => validateBootstrapResponse(nonRunningStop)).toThrow(
      /current Running task/,
    );
  });

  it("rejects a non-deterministic storage aggregate and non-ready admission", () => {
    const bootstrap = validBootstrap();
    const wrongAggregate = {
      ...bootstrap,
      scheduler: {
        ...bootstrap.scheduler,
        storage: {
          ...bootstrap.scheduler.storage,
          state: "normal",
          runtime: { state: "unavailable" },
        },
      },
    };
    const runningWhileQuiescing = {
      ...bootstrap,
      service_state: "quiescing",
    };

    expect(() => validateBootstrapResponse(wrongAggregate)).toThrow(
      /logical scope aggregate unavailable/,
    );
    expect(() => validateBootstrapResponse(runningWhileQuiescing)).toThrow(
      /must be paused/,
    );
  });
});

describe("live scheduler authority validation", () => {
  it("accepts a retained terminal permit above the durable Running count", () => {
    const bootstrap = retainedTerminalPermitBootstrap();

    expect(
      validateSchedulerStateAgainstAuthority(bootstrap.scheduler, {
        repositories: bootstrap.repositories,
        tasks: bootstrap.tasks,
        serviceState: bootstrap.service_state,
      }),
    ).toBe(bootstrap.scheduler);
  });

  it("rejects digest-valid projections that disagree with current tasks, repositories, or service state", () => {
    const bootstrap = validBootstrap();
    const context = {
      repositories: bootstrap.repositories,
      tasks: bootstrap.tasks,
      serviceState: bootstrap.service_state,
    };

    expect(() =>
      validateSchedulerStateAgainstAuthority(
        {
          ...bootstrap.scheduler,
          active_task_count: 0,
          stopping_tasks: [],
        },
        context,
      ),
    ).toThrow(/current Running tasks/);
    expect(() =>
      validateSchedulerStateAgainstAuthority(
        {
          ...bootstrap.scheduler,
          queued_tasks: [...bootstrap.scheduler.queued_tasks].reverse(),
        },
        context,
      ),
    ).toThrow(/authoritative/);
    expect(() =>
      validateSchedulerStateAgainstAuthority(
        {
          ...bootstrap.scheduler,
          stopping_tasks: [
            {
              task_id: NON_RUNNING_TASK,
              intent: "disk_pressure_critical",
            },
          ],
        },
        context,
      ),
    ).toThrow(/current Running task/);
    expect(() =>
      validateSchedulerStateAgainstAuthority(
        {
          ...bootstrap.scheduler,
          storage: {
            ...bootstrap.scheduler.storage,
            repositories: bootstrap.scheduler.storage.repositories.slice(0, 1),
          },
        },
        context,
      ),
    ).toThrow(/cover Bootstrap repositories/);
    expect(() =>
      validateSchedulerStateAgainstAuthority(bootstrap.scheduler, {
        ...context,
        serviceState: "quiescing",
      }),
    ).toThrow(/service_state/);
  });
});

describe("logical scheduler storage aggregation", () => {
  it("uses critical, unavailable, pressure, normal priority", () => {
    expect(aggregateStorageState(["normal", "pressure"])).toBe("pressure");
    expect(aggregateStorageState(["pressure", "unavailable"])).toBe(
      "unavailable",
    );
    expect(aggregateStorageState(["unavailable", "critical"])).toBe(
      "critical",
    );
    expect(aggregateStorageState([])).toBe("normal");
  });
});
