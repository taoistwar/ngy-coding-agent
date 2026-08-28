import { describe, expect, it } from "vitest";

import type {
  DeliveryCleanupOperationEnvelope,
  DeliveryMergeOperationEnvelope,
  DeliveryTask,
} from "../api/types";
import {
  DELIVERY_INITIAL_POLL_DELAY_MS,
  initialDeliveryState,
} from "./deliveryModel";
import { deliveryReducer } from "./deliveryReducer";

const TASK_A = "11111111-1111-4111-8111-111111111111";
const TASK_B = "22222222-2222-4222-8222-222222222222";
const OPERATION_ID = "33333333-3333-4333-8333-333333333333";
const CLEANUP_ID = "44444444-4444-4444-8444-444444444444";
const OID_A = "1".repeat(40);
const OID_B = "2".repeat(40);
const FINGERPRINT = "a".repeat(64);

function operation(
  version: number,
  state: DeliveryMergeOperationEnvelope["state"] = "accepted",
): DeliveryMergeOperationEnvelope {
  return {
    kind: "merge",
    operation_id: OPERATION_ID,
    version,
    state,
    review_generation: 7,
    workspace_fingerprint: FINGERPRINT,
    candidate_source_tree: OID_B,
    preflight_source_commit: OID_B,
    source_commit:
      state === "merge_pending" || state === "merged" ? OID_B : null,
    target_branch: "refs/heads/main",
    target_head: OID_A,
    conflicts: null,
    failure: null,
  };
}

function projection(taskId: string, latest = operation(1)): DeliveryTask {
  const { kind: _, ...latestMerge } = latest;
  return {
    task_id: taskId,
    eligibility: "eligible",
    reasons: [],
    evidence: { review_generation: 7, workspace_fingerprint: FINGERPRINT },
    target: { available: true, branch: "refs/heads/main", head: OID_A },
    source: null,
    latest_merge: latestMerge,
    latest_cleanup: null,
    disposition: null,
    allowed_actions: [],
  };
}

function cleanupOperation(
  version: number,
  state: DeliveryCleanupOperationEnvelope["state"] = "remove_pending",
): DeliveryCleanupOperationEnvelope {
  return {
    kind: "cleanup",
    operation_id: CLEANUP_ID,
    cleanup_kind: "remove_worktree",
    version,
    state,
    expected_disposition_version: 1,
    expected_merge_operation_id: OPERATION_ID,
    expected_source_ref: "refs/heads/coding-agent/task",
    expected_source_oid: OID_B,
    target_branch: null,
    target_head: null,
    failure: null,
  };
}

describe("deliveryReducer", () => {
  it("rebuilds polling from GET delivery and contains no lifecycle cursor state", () => {
    let state = deliveryReducer(initialDeliveryState, {
      type: "task.selected",
      taskId: TASK_A,
      generation: 1,
    });
    state = deliveryReducer(state, {
      type: "delivery.received",
      taskId: TASK_A,
      generation: 1,
      projection: projection(TASK_A),
    });

    expect(state).toMatchObject({
      taskId: TASK_A,
      phase: "polling",
      trackedOperationId: OPERATION_ID,
      pollDelayMs: 500,
      operation: { kind: "merge", operation_id: OPERATION_ID, version: 1 },
    });
    expect(state).not.toHaveProperty("applied_task_event_id");
    expect(state).not.toHaveProperty("appliedEventId");
    expect(state).not.toHaveProperty("scheduler");
  });

  it("rebuilds a pending cleanup from the required-nullable task projection", () => {
    const durable = projection(TASK_A, operation(9, "merged"));
    const { kind: _, ...latestCleanup } = cleanupOperation(4);
    durable.latest_cleanup = latestCleanup;
    let state = deliveryReducer(initialDeliveryState, {
      type: "task.selected",
      taskId: TASK_A,
      generation: 1,
    });
    state = deliveryReducer(state, {
      type: "delivery.received",
      taskId: TASK_A,
      generation: 1,
      projection: durable,
    });

    expect(state).toMatchObject({
      phase: "polling",
      trackedOperationId: CLEANUP_ID,
      operation: {
        kind: "cleanup",
        operation_id: CLEANUP_ID,
        version: 4,
        state: "remove_pending",
      },
    });
  });

  it("ignores stale task responses and clears modal state on task switch", () => {
    let state = deliveryReducer(initialDeliveryState, {
      type: "task.selected",
      taskId: TASK_A,
      generation: 1,
    });
    state = deliveryReducer(state, {
      type: "modal.opened",
      modal: {
        kind: "preflight",
        taskId: TASK_A,
        operationId: null,
        operationVersion: null,
        authority: null,
      },
    });
    state = deliveryReducer(state, {
      type: "task.selected",
      taskId: TASK_B,
      generation: 2,
    });
    const switched = state;
    state = deliveryReducer(state, {
      type: "delivery.received",
      taskId: TASK_A,
      generation: 1,
      projection: projection(TASK_A),
    });

    expect(state).toBe(switched);
    expect(state).toMatchObject({ taskId: TASK_B, modal: null, operation: null });
  });

  it("never regresses an operation version, resets progress backoff, and clears stale modal", () => {
    let state = deliveryReducer(initialDeliveryState, {
      type: "task.selected",
      taskId: TASK_A,
      generation: 1,
    });
    state = deliveryReducer(state, {
      type: "operation.tracked",
      taskId: TASK_A,
      generation: 1,
      operation: operation(1),
    });
    state = deliveryReducer(state, {
      type: "modal.opened",
      modal: {
        kind: "merge",
        taskId: TASK_A,
        operationId: OPERATION_ID,
        operationVersion: 1,
        authority: null,
      },
    });
    state = deliveryReducer(state, {
      type: "operation.received",
      taskId: TASK_A,
      generation: 1,
      operation: operation(1),
    });
    expect(state.pollDelayMs).toBe(1_000);
    expect(state.modal).not.toBeNull();

    state = deliveryReducer(state, {
      type: "operation.received",
      taskId: TASK_A,
      generation: 1,
      operation: operation(2, "merge_pending"),
    });
    expect(state.pollDelayMs).toBe(DELIVERY_INITIAL_POLL_DELAY_MS);
    expect(state.modal).toBeNull();
    const progressed = state;

    state = deliveryReducer(state, {
      type: "operation.received",
      taskId: TASK_A,
      generation: 1,
      operation: operation(1),
    });
    expect(state).toBe(progressed);
    expect(state.operation?.version).toBe(2);

    state = deliveryReducer(state, {
      type: "operation.received",
      taskId: TASK_A,
      generation: 1,
      operation: operation(3, "merged"),
    });
    expect(state).toMatchObject({ phase: "refreshing", pollDelayMs: 500 });
  });

  it("clears a confirmation when its evidence or target authority becomes stale", () => {
    const ready = operation(3, "preflight_ready");
    let state = deliveryReducer(initialDeliveryState, {
      type: "task.selected",
      taskId: TASK_A,
      generation: 1,
    });
    state = deliveryReducer(state, {
      type: "delivery.received",
      taskId: TASK_A,
      generation: 1,
      projection: projection(TASK_A, ready),
    });
    state = deliveryReducer(state, {
      type: "modal.opened",
      modal: {
        kind: "merge",
        taskId: TASK_A,
        operationId: OPERATION_ID,
        operationVersion: 3,
        authority: {
          reviewGeneration: 7,
          workspaceFingerprint: FINGERPRINT,
          targetBranch: "refs/heads/main",
          targetHead: OID_A,
        },
      },
    });
    expect(state.modal).not.toBeNull();

    const changedTarget = projection(TASK_A, ready);
    changedTarget.target = {
      available: true,
      branch: "refs/heads/main",
      head: "3".repeat(40),
    };
    state = deliveryReducer(state, {
      type: "delivery.received",
      taskId: TASK_A,
      generation: 1,
      projection: changedTarget,
    });
    expect(state.modal).toBeNull();
  });

  it("caps retry backoff at two seconds", () => {
    let state = deliveryReducer(initialDeliveryState, {
      type: "task.selected",
      taskId: TASK_A,
      generation: 1,
    });
    state = deliveryReducer(state, {
      type: "operation.tracked",
      taskId: TASK_A,
      generation: 1,
      operation: operation(1),
    });
    for (let index = 0; index < 5; index += 1) {
      state = deliveryReducer(state, {
        type: "operation.received",
        taskId: TASK_A,
        generation: 1,
        operation: operation(1),
      });
    }
    expect(state.pollDelayMs).toBe(2_000);
  });
});
