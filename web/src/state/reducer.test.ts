import { describe, expect, it } from "vitest";

import type {
  ActivityEntry,
  BootstrapResponse,
  DiffSnapshot,
  PlanSnapshot,
  Repository,
  ReviewEvidence,
  Task,
  TaskDetail,
  TaskEvent,
  TestSnapshot,
} from "../api/types";
import { initialAgentState } from "./model";
import { agentReducer, inspectEventProjection } from "./reducer";

const NOW = "2026-07-15T00:00:00Z";
const REVIEW_TASK_ID = "11111111-1111-4111-8111-111111111111";

function repository(id: string): Repository {
  return {
    id,
    display_name: `repo-${id}`,
    selected_path: `C:/${id}`,
    git_root: `C:/${id}`,
    cargo_workspace_root: `C:/${id}`,
    created_at: NOW,
    last_opened_at: NOW,
  };
}

function task(
  id: string,
  status: Task["status"] = "running",
  lastEventId = 10,
): Task {
  return {
    id,
    repository_id: "repo-1",
    client_request_id: `request-${id}`,
    prompt: `work on ${id}`,
    status,
    delivery_readiness: "unreviewed",
    attempt: 1,
    last_event_id: lastEventId,
    created_at: NOW,
    retry_of: null,
    started_at: null,
    finished_at: null,
    failure: null,
  };
}

function bootstrap(overrides: Partial<BootstrapResponse> = {}): BootstrapResponse {
  return {
    csrf_token: "csrf",
    latest_event_id: 10,
    max_concurrent_tasks: 4,
    repositories: [repository("repo-1"), repository("repo-2")],
    server_started_at: NOW,
    service_state: "ready",
    service_state_generation: 7,
    tasks: [task("task-1"), task("task-2", "queued")],
    ...overrides,
  };
}

function detail(value: Task, cursor = 10): TaskDetail {
  return {
    task: value,
    event_cursor: cursor,
    plan: null,
    activity: [],
    diff: null,
    tests: null,
    timeline: [],
    reviews: [],
  };
}

function reviewPlan(): PlanSnapshot {
  return {
    format_version: 1,
    revision: 1,
    summary: "Implement and verify the requested change.",
    items: [
      {
        id: "plan-item-1",
        title: "Implement",
        description: "Implement the requested change.",
        acceptance_criteria: ["The required test passes."],
        status: "running",
      },
    ],
    initial_required_checks: [
      {
        id: "check-cargo-test",
        kind: "cargo_test",
        package: null,
        integration_test: null,
      },
    ],
  };
}

function reviewTask(lastEventId = 10): Task {
  return {
    ...task(REVIEW_TASK_ID, "running", lastEventId),
    repository_id: "22222222-2222-4222-8222-222222222222",
    client_request_id: "33333333-3333-4333-8333-333333333333",
    started_at: NOW,
  };
}

function reviewDetail(reviews: ReviewEvidence[] = [], cursor = 10): TaskDetail {
  return {
    ...detail(reviewTask(cursor), cursor),
    plan: reviewPlan(),
    reviews,
  };
}

function reviewEvidence(round: number): ReviewEvidence {
  const workspaceDigest = {
    algorithm: "workspace_fingerprint_v1" as const,
    value: "a".repeat(64),
  };
  const requiredCheck = {
    id: "check-cargo-test",
    kind: "cargo_test" as const,
    package: null,
    integration_test: null,
  };
  return {
    round,
    decision_source: "reviewer",
    workspace_generation: 3,
    workspace_digest: workspaceDigest,
    verdict: round === 1 ? "changes_requested" : "approved",
    summary:
      round === 1
        ? "A blocking issue remains."
        : `Approved in round ${round}`,
    findings:
      round === 1
        ? [
            {
              id: "review-1-finding-1",
              severity: "blocking",
              message: "Fix the blocking issue.",
              path: "src/lib.rs",
              line: 7,
            },
          ]
        : [],
    added_required_checks: [],
    required_checks: [requiredCheck],
    check_evidence: [
      {
        check_id: requiredCheck.id,
        actor: "executor",
        role_run: round,
        workspace_generation: 3,
        workspace_digest: workspaceDigest,
        status: round === 1 ? "failed" : "passed",
        duration_ms: 12,
        summary: "passed",
        truncated: false,
      },
    ],
    coverage: {
      generation: 3,
      workspace_digest: workspaceDigest,
      manifest_sha256: "b".repeat(64),
      covered_chunks: [],
      total_chunks: 0,
    },
    created_at: NOW,
  };
}

function reviewEvent(
  id: number,
  review: ReviewEvidence,
  taskId = "task-1",
): Extract<TaskEvent, { kind: "review.updated" }> {
  return {
    id,
    schema_version: 1,
    task_id: taskId,
    kind: "review.updated",
    created_at: NOW,
    payload: { review },
  };
}

function lifecycleEvent(
  id: number,
  value: Task,
  kind:
    | "task.queued"
    | "task.started"
    | "task.completed"
    | "task.failed"
    | "task.cancelled"
    | "task.interrupted",
): TaskEvent {
  return {
    id,
    schema_version: 1,
    task_id: value.id,
    kind,
    created_at: NOW,
    payload: { task: value },
  } as TaskEvent;
}

describe("agentReducer", () => {
  it("normalizes bootstrap collections by id while preserving order", () => {
    const state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap(),
    });

    expect(state.repositoryOrder).toEqual(["repo-1", "repo-2"]);
    expect(state.repositoriesById["repo-2"]?.display_name).toBe("repo-repo-2");
    expect(state.taskOrder).toEqual(["task-1", "task-2"]);
    expect(state.tasksById["task-1"]?.status).toBe("running");
    expect(state.appliedEventId).toBe(10);
  });

  it("ignores duplicate persisted ids and marks older ids as protocol recovery", () => {
    const ready = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap(),
    });
    const duplicate = agentReducer(ready, {
      type: "event.received",
      event: lifecycleEvent(10, task("task-1", "completed", 10), "task.completed"),
    });
    const outOfOrder = agentReducer(ready, {
      type: "event.received",
      event: lifecycleEvent(9, task("task-1", "completed", 9), "task.completed"),
    });

    expect(duplicate).toBe(ready);
    expect(outOfOrder.connection).toBe("protocol_error");
    expect(outOfOrder.recoveryReason).toBe("non_monotonic_event_id");
    expect(outOfOrder.tasksById["task-1"]?.status).toBe("running");
  });

  it("replaces snapshot panels, deduplicates activity ids, and advances detail cursor", () => {
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap(),
    });
    state = agentReducer(state, { type: "task.selected", taskId: "task-1" });
    state = agentReducer(state, {
      type: "detail.received",
      taskId: "task-1",
      generation: state.selectionGeneration,
      detail: detail(task("task-1")),
    });

    const plan: PlanSnapshot = {
      format_version: 1,
      revision: 2,
      summary: "Ship the requested change",
      items: [
        {
          id: "p-1",
          title: "ship",
          description: "Implement the requested change",
          acceptance_criteria: ["The focused tests pass"],
          status: "running",
        },
      ],
      initial_required_checks: [],
    };
    const diff: DiffSnapshot = {
      revision: 3,
      files: [
        {
          path: "src/lib.rs",
          status: "modified",
          patch: "+done",
          additions: 1,
          deletions: 0,
          truncated: false,
        },
      ],
    };
    const tests: TestSnapshot = {
      revision: 4,
      status: "passed",
      cases: [
        {
          id: "case-1",
          name: "works",
          status: "passed",
          duration_ms: 2,
          summary: "ok",
        },
      ],
    };
    const entry: ActivityEntry = {
      id: "activity-1",
      level: "info",
      actor: "executor",
      role_run: 1,
      message: "done",
      created_at: NOW,
    };

    const events: TaskEvent[] = [
      {
        id: 11,
        schema_version: 1,
        task_id: "task-1",
        kind: "plan.updated",
        created_at: NOW,
        payload: { plan },
      },
      {
        id: 12,
        schema_version: 1,
        task_id: "task-1",
        kind: "activity.appended",
        created_at: NOW,
        payload: { entry },
      },
      {
        id: 13,
        schema_version: 1,
        task_id: "task-1",
        kind: "activity.appended",
        created_at: NOW,
        payload: { entry },
      },
      {
        id: 14,
        schema_version: 1,
        task_id: "task-1",
        kind: "diff.updated",
        created_at: NOW,
        payload: { diff },
      },
      {
        id: 15,
        schema_version: 1,
        task_id: "task-1",
        kind: "test.updated",
        created_at: NOW,
        payload: { tests },
      },
    ];

    for (const event of events) {
      state = agentReducer(state, { type: "event.received", event });
    }

    expect(state.selectedDetail?.plan).toEqual(plan);
    expect(state.selectedDetail?.activity).toEqual([entry]);
    expect(state.selectedDetail?.diff).toEqual(diff);
    expect(state.selectedDetail?.tests).toEqual(tests);
    expect(state.selectedDetail?.event_cursor).toBe(15);
  });

  it("appends reviews, deduplicates an exact round replay, and leaves lifecycle quality state authoritative", () => {
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap({ tasks: [reviewTask()] }),
    });
    state = agentReducer(state, {
      type: "task.selected",
      taskId: REVIEW_TASK_ID,
    });
    state = agentReducer(state, {
      type: "detail.received",
      taskId: REVIEW_TASK_ID,
      generation: state.selectionGeneration,
      detail: reviewDetail(),
    });

    const first = reviewEvidence(1);
    const second = reviewEvidence(2);
    state = agentReducer(state, {
      type: "event.received",
      event: reviewEvent(11, first, REVIEW_TASK_ID),
    });
    state = agentReducer(state, {
      type: "event.received",
      event: reviewEvent(12, structuredClone(first), REVIEW_TASK_ID),
    });
    state = agentReducer(state, {
      type: "event.received",
      event: reviewEvent(13, second, REVIEW_TASK_ID),
    });

    expect(state.selectedDetail?.reviews).toEqual([first, second]);
    expect(state.selectedDetail?.event_cursor).toBe(13);
    expect(state.appliedEventId).toBe(13);
    expect(state.selectedDetail?.task.status).toBe("running");
    expect(state.selectedDetail?.task.delivery_readiness).toBe("unreviewed");
    expect(state.tasksById[REVIEW_TASK_ID]?.status).toBe("running");
    expect(state.tasksById[REVIEW_TASK_ID]?.delivery_readiness).toBe(
      "unreviewed",
    );
  });

  it("does not commit a conflicting review cursor before authoritative recovery", () => {
    const first = reviewEvidence(1);
    const conflicting = {
      ...structuredClone(first),
      summary: "A different payload for the same review round.",
    };
    const conflictEvent = reviewEvent(11, conflicting, REVIEW_TASK_ID);
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap({ tasks: [reviewTask()] }),
    });
    state = agentReducer(state, {
      type: "task.selected",
      taskId: REVIEW_TASK_ID,
    });
    state = agentReducer(state, {
      type: "detail.received",
      taskId: REVIEW_TASK_ID,
      generation: state.selectionGeneration,
      detail: reviewDetail([first]),
    });

    const preflight = inspectEventProjection(state, conflictEvent);
    expect(preflight).toEqual({
      kind: "conflict",
      reason: "review_payload_conflict",
    });
    state = agentReducer(state, {
      type: "event.conflicted",
      event: conflictEvent,
      reason: "review_payload_conflict",
    });

    expect(state.appliedEventId).toBe(10);
    expect(state.selectedDetail?.reviews).toEqual([first]);
    expect(state.snapshotRecovery).toEqual({
      conflictEventId: 11,
      reason: "review_payload_conflict",
    });
    expect(state.recoveryBuffer.map(({ id }) => id)).toEqual([11]);
    expect(state.connection).toBe("recovering");
    expect(state.diagnostics.at(-1)).toEqual(
      expect.objectContaining({ id: 11, kind: "review.updated" }),
    );

    state = agentReducer(state, {
      type: "recovery.received",
      bootstrap: bootstrap({
        latest_event_id: 11,
        tasks: [reviewTask(11)],
      }),
      bufferedEvents: [conflictEvent],
    });

    expect(state.appliedEventId).toBe(11);
    expect(state.snapshotRecovery).toBeNull();
    expect(state.recoveryBuffer).toEqual([]);
    expect(state.selectedDetail).toBeNull();
    expect(state.detailLoading).toBe(true);
  });

  it("marks a round gap stale, commits its normal cursor, and replaces it at the detail boundary", () => {
    const second = reviewEvidence(2);
    const gapEvent = reviewEvent(11, second, REVIEW_TASK_ID);
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap({ tasks: [reviewTask()] }),
    });
    state = agentReducer(state, {
      type: "task.selected",
      taskId: REVIEW_TASK_ID,
    });
    state = agentReducer(state, {
      type: "detail.received",
      taskId: REVIEW_TASK_ID,
      generation: state.selectionGeneration,
      detail: reviewDetail(),
    });

    state = agentReducer(state, {
      type: "event.received",
      event: gapEvent,
    });

    expect(state.appliedEventId).toBe(11);
    expect(state.detailStale).toBe(true);
    expect(state.detailLoading).toBe(true);
    expect(state.selectedDetail?.event_cursor).toBe(11);
    expect(state.selectedDetail?.reviews).toEqual([]);
    expect(state.selectedDetail?.task.status).toBe("running");
    expect(state.selectedDetail?.task.delivery_readiness).toBe("unreviewed");

    state = agentReducer(state, {
      type: "detail.received",
      taskId: REVIEW_TASK_ID,
      generation: state.selectionGeneration,
      detail: reviewDetail([reviewEvidence(1), second], 11),
    });

    expect(state.detailStale).toBe(false);
    expect(state.detailLoading).toBe(false);
    expect(state.selectedDetail?.reviews.map(({ round }) => round)).toEqual([
      1, 2,
    ]);
  });

  it("atomically discards recovery events through the watermark and applies only later ids", () => {
    const first = reviewEvidence(1);
    const conflictEvent = reviewEvent(
      11,
      { ...structuredClone(first), summary: "Conflicting payload." },
      REVIEW_TASK_ID,
    );
    const laterEvent: TaskEvent = {
      id: 12,
      schema_version: 1,
      task_id: REVIEW_TASK_ID,
      kind: "activity.appended",
      created_at: NOW,
      payload: {
        entry: {
          id: "activity-after-recovery",
          level: "info",
          actor: "system",
          role_run: null,
          message: "Committed after the recovery snapshot.",
          created_at: NOW,
        },
      },
    };
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap({ tasks: [reviewTask()] }),
    });
    state = agentReducer(state, {
      type: "task.selected",
      taskId: REVIEW_TASK_ID,
    });
    state = agentReducer(state, {
      type: "detail.received",
      taskId: REVIEW_TASK_ID,
      generation: state.selectionGeneration,
      detail: reviewDetail([first]),
    });
    state = agentReducer(state, {
      type: "event.conflicted",
      event: conflictEvent,
      reason: "review_payload_conflict",
    });
    state = agentReducer(state, {
      type: "event.received",
      event: laterEvent,
    });

    expect(state.appliedEventId).toBe(10);
    expect(state.recoveryBuffer.map(({ id }) => id)).toEqual([11, 12]);

    state = agentReducer(state, {
      type: "recovery.received",
      bootstrap: bootstrap({
        latest_event_id: 11,
        tasks: [reviewTask(11)],
      }),
      bufferedEvents: [conflictEvent, laterEvent],
    });

    expect(state.appliedEventId).toBe(12);
    expect(state.tasksById[REVIEW_TASK_ID]?.last_event_id).toBe(12);
    expect(state.liveBufferByTaskId[REVIEW_TASK_ID]?.map(({ id }) => id)).toEqual([
      12,
    ]);
    expect(state.snapshotRecovery).toBeNull();
  });

  it("keeps recovery pending when a post-watermark buffer cannot be projected", () => {
    const first = reviewEvidence(1);
    const conflictEvent = reviewEvent(
      11,
      { ...structuredClone(first), summary: "Conflicting payload." },
      REVIEW_TASK_ID,
    );
    const invalidLaterEvent = {
      id: 12,
      schema_version: 2,
      task_id: REVIEW_TASK_ID,
      kind: "activity.appended",
      created_at: NOW,
      payload: {
        entry: {
          id: "invalid-schema-event",
          level: "info",
          actor: "system",
          role_run: null,
          message: "Must not be committed.",
          created_at: NOW,
        },
      },
    } as TaskEvent;
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap({ tasks: [reviewTask()] }),
    });
    state = agentReducer(state, {
      type: "task.selected",
      taskId: REVIEW_TASK_ID,
    });
    state = agentReducer(state, {
      type: "detail.received",
      taskId: REVIEW_TASK_ID,
      generation: state.selectionGeneration,
      detail: reviewDetail([first]),
    });
    state = agentReducer(state, {
      type: "event.conflicted",
      event: conflictEvent,
      reason: "review_payload_conflict",
    });

    const recovered = agentReducer(state, {
      type: "recovery.received",
      bootstrap: bootstrap({
        latest_event_id: 11,
        tasks: [reviewTask(11)],
      }),
      bufferedEvents: [conflictEvent, invalidLaterEvent],
    });

    expect(recovered.appliedEventId).toBe(10);
    expect(recovered.snapshotRecovery).toEqual(state.snapshotRecovery);
    expect(recovered.connection).toBe("recovering");
    expect(recovered.tasksById[REVIEW_TASK_ID]).toEqual(
      state.tasksById[REVIEW_TASK_ID],
    );
  });

  it("updates lifecycle summaries and timeline, including non-selected tasks", () => {
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap(),
    });
    state = agentReducer(state, { type: "task.selected", taskId: "task-1" });
    state = agentReducer(state, {
      type: "detail.received",
      taskId: "task-1",
      generation: state.selectionGeneration,
      detail: detail(task("task-1")),
    });

    state = agentReducer(state, {
      type: "event.received",
      event: lifecycleEvent(
        11,
        task("task-2", "completed", 11),
        "task.completed",
      ),
    });
    state = agentReducer(state, {
      type: "event.received",
      event: lifecycleEvent(
        12,
        task("task-1", "completed", 12),
        "task.completed",
      ),
    });

    expect(state.tasksById["task-2"]?.status).toBe("completed");
    expect(state.tasksById["task-1"]?.status).toBe("completed");
    expect(state.selectedDetail?.timeline).toEqual([
      expect.objectContaining({ event_id: 12, kind: "task.completed" }),
    ]);
  });

  it("does not regress a summary when a REST detail snapshot is ahead of SSE", () => {
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap({
        latest_event_id: 5,
        tasks: [task("task-1", "running", 5)],
      }),
    });
    state = agentReducer(state, { type: "task.selected", taskId: "task-1" });
    state = agentReducer(state, {
      type: "detail.received",
      taskId: "task-1",
      generation: state.selectionGeneration,
      detail: detail(task("task-1", "completed", 20), 20),
    });

    state = agentReducer(state, {
      type: "event.received",
      event: lifecycleEvent(
        10,
        task("task-1", "running", 10),
        "task.started",
      ),
    });

    expect(state.appliedEventId).toBe(10);
    expect(state.tasksById["task-1"]?.status).toBe("completed");
    expect(state.tasksById["task-1"]?.last_event_id).toBe(20);
    expect(state.selectedDetail?.task.status).toBe("completed");
  });

  it("keeps service generation monotonic", () => {
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap(),
    });
    state = agentReducer(state, {
      type: "service.received",
      state: "store_degraded",
      generation: 9,
    });
    state = agentReducer(state, {
      type: "service.received",
      state: "ready",
      generation: 8,
    });

    expect(state.serviceState).toBe("store_degraded");
    expect(state.serviceGeneration).toBe(9);
  });

  it("records a supported unknown event diagnostic and advances its cursor", () => {
    const ready = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap(),
    });
    const state = agentReducer(ready, {
      type: "event.unknown",
      id: 11,
      kind: "future.panel.updated",
      schemaVersion: 1,
    });

    expect(state.appliedEventId).toBe(11);
    expect(state.diagnostics).toEqual([
      expect.objectContaining({ id: 11, kind: "future.panel.updated" }),
    ]);
  });

  it("replays only buffered events above the detail cursor in sorted order", () => {
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap({ latest_event_id: 9 }),
    });
    state = agentReducer(state, { type: "task.selected", taskId: "task-1" });
    const generation = state.selectionGeneration;

    state = agentReducer(state, {
      type: "event.received",
      event: lifecycleEvent(10, task("task-1", "running", 10), "task.started"),
    });
    state = agentReducer(state, {
      type: "event.received",
      event: lifecycleEvent(
        11,
        task("task-1", "completed", 11),
        "task.completed",
      ),
    });
    state = agentReducer(state, {
      type: "detail.received",
      taskId: "task-1",
      generation,
      detail: detail(task("task-1", "running", 10), 10),
    });

    expect(state.selectedDetail?.task.status).toBe("completed");
    expect(state.selectedDetail?.timeline.map((entry) => entry.event_id)).toEqual([11]);
    expect(state.selectedDetail?.event_cursor).toBe(11);
    expect(state.liveBufferByTaskId["task-1"]).toEqual([]);
  });

  it("rolls back cancel optimism on STORE_BUSY and on a competing terminal event", () => {
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap(),
    });
    state = agentReducer(state, { type: "cancel.started", taskId: "task-1" });
    expect(state.commands.cancelByTaskId["task-1"]).toEqual(
      expect.objectContaining({ phase: "pending", optimistic: true }),
    );

    state = agentReducer(state, {
      type: "cancel.failed",
      taskId: "task-1",
      error: {
        code: "STORE_BUSY",
        message: "busy",
        retryable: true,
        requestId: null,
      },
    });
    expect(state.commands.cancelByTaskId["task-1"]).toEqual(
      expect.objectContaining({ phase: "error", optimistic: false }),
    );
    expect(state.tasksById["task-1"]?.status).toBe("running");

    state = agentReducer(state, { type: "cancel.started", taskId: "task-1" });
    state = agentReducer(state, {
      type: "event.received",
      event: lifecycleEvent(
        11,
        task("task-1", "completed", 11),
        "task.completed",
      ),
    });
    expect(state.commands.cancelByTaskId["task-1"]).toBeUndefined();
    expect(state.tasksById["task-1"]?.status).toBe("completed");

    state = agentReducer(state, {
      type: "cancel.failed",
      taskId: "task-1",
      error: {
        code: "STORE_BUSY",
        message: "late response",
        retryable: true,
        requestId: null,
      },
    });
    expect(state.commands.cancelByTaskId["task-1"]).toBeUndefined();
    expect(state.tasksById["task-1"]?.status).toBe("completed");
  });

  it("upserts REST repositories and tasks without duplicating normalized order", () => {
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap(),
    });
    state = agentReducer(state, {
      type: "repository.upserted",
      repository: repository("repo-3"),
    });
    state = agentReducer(state, {
      type: "repository.upserted",
      repository: { ...repository("repo-3"), display_name: "renamed" },
    });
    state = agentReducer(state, {
      type: "task.upserted",
      task: task("task-3", "queued", 11),
    });
    state = agentReducer(state, {
      type: "task.upserted",
      task: task("task-3", "running", 12),
    });

    expect(state.repositoryOrder).toEqual(["repo-1", "repo-2", "repo-3"]);
    expect(state.repositoriesById["repo-3"]?.display_name).toBe("renamed");
    expect(state.taskOrder).toEqual(["task-1", "task-2", "task-3"]);
    expect(state.tasksById["task-3"]?.status).toBe("running");
  });

  it("reconciles ephemeral cancel commands against an authoritative bootstrap", () => {
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap(),
    });
    state = agentReducer(state, {
      type: "task.upserted",
      task: task("task-3", "running", 10),
    });
    state = agentReducer(state, { type: "cancel.started", taskId: "task-1" });
    state = agentReducer(state, { type: "cancel.started", taskId: "task-2" });
    state = agentReducer(state, { type: "cancel.started", taskId: "task-3" });

    state = agentReducer(state, {
      type: "bootstrap.received",
      bootstrap: bootstrap({
        tasks: [
          task("task-1", "completed", 11),
          task("task-3", "running", 10),
        ],
      }),
    });

    expect(state.commands.cancelByTaskId["task-1"]).toBeUndefined();
    expect(state.commands.cancelByTaskId["task-2"]).toBeUndefined();
    expect(state.commands.cancelByTaskId["task-3"]?.phase).toBe("pending");
  });

  it("clears ephemeral commands when the session expires", () => {
    let state = agentReducer(initialAgentState, {
      type: "bootstrap.received",
      bootstrap: bootstrap(),
    });
    state = agentReducer(state, { type: "cancel.started", taskId: "task-1" });
    state = agentReducer(state, { type: "session.expired" });

    expect(state.connection).toBe("session_expired");
    expect(state.commands.cancelByTaskId).toEqual({});
  });
});
