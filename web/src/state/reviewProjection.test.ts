import { describe, expect, it } from "vitest";

import type {
  PlanSnapshot,
  RequiredCheck,
  ReviewEvidence,
  Task,
  TaskDetail,
  TaskEvent,
  WorkspaceDigest,
} from "../api/types";
import { projectReviewEvent } from "./reviewProjection";

const NOW = "2026-07-23T00:00:00Z";
const TASK_ID = "11111111-1111-4111-8111-111111111111";
const REPOSITORY_ID = "22222222-2222-4222-8222-222222222222";
const CLIENT_REQUEST_ID = "33333333-3333-4333-8333-333333333333";

function digest(value: string): WorkspaceDigest {
  return {
    algorithm: "workspace_fingerprint_v1",
    value: value.repeat(64),
  };
}

function requiredCheck(id = "required-test"): RequiredCheck {
  return {
    id,
    kind: "cargo_test",
    package: null,
    integration_test: null,
  };
}

function plan(): PlanSnapshot {
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
    initial_required_checks: [requiredCheck()],
  };
}

function task(lastEventId = 10): Task {
  return {
    id: TASK_ID,
    repository_id: REPOSITORY_ID,
    client_request_id: CLIENT_REQUEST_ID,
    prompt: "Implement the requested change.",
    status: "running",
    delivery_readiness: "unreviewed",
    attempt: 1,
    last_event_id: lastEventId,
    created_at: NOW,
    retry_of: null,
    started_at: NOW,
    finished_at: null,
    failure: null,
  };
}

function changesRequestedReview(round = 1): ReviewEvidence {
  const workspaceDigest = digest("a");
  return {
    round,
    decision_source: "reviewer",
    workspace_generation: round,
    workspace_digest: workspaceDigest,
    verdict: "changes_requested",
    summary: "A blocking issue remains.",
    findings: [
      {
        id: `review-${round}-finding-1`,
        severity: "blocking",
        message: "Fix the blocking issue.",
        path: "src/lib.rs",
        line: 7,
      },
    ],
    added_required_checks: [],
    required_checks: [requiredCheck()],
    check_evidence: [
      {
        check_id: "required-test",
        actor: "executor",
        role_run: round,
        workspace_generation: round,
        workspace_digest: workspaceDigest,
        status: "failed",
        duration_ms: 12,
        summary: "The required test failed.",
        truncated: false,
      },
    ],
    coverage: {
      generation: round,
      workspace_digest: workspaceDigest,
      manifest_sha256: "b".repeat(64),
      covered_chunks: [0],
      total_chunks: 1,
    },
    created_at: NOW,
  };
}

function approvedReview(round = 2): ReviewEvidence {
  const workspaceDigest = digest("c");
  return {
    round,
    decision_source: "reviewer",
    workspace_generation: round,
    workspace_digest: workspaceDigest,
    verdict: "approved",
    summary: "All required evidence is current.",
    findings: [],
    added_required_checks: [],
    required_checks: [requiredCheck()],
    check_evidence: [
      {
        check_id: "required-test",
        actor: "executor",
        role_run: round,
        workspace_generation: round,
        workspace_digest: workspaceDigest,
        status: "passed",
        duration_ms: 9,
        summary: "The required test passed.",
        truncated: false,
      },
    ],
    coverage: {
      generation: round,
      workspace_digest: workspaceDigest,
      manifest_sha256: "d".repeat(64),
      covered_chunks: [0],
      total_chunks: 1,
    },
    created_at: NOW,
  };
}

function detail(reviews: ReviewEvidence[] = []): TaskDetail {
  return {
    task: task(),
    event_cursor: 10,
    plan: plan(),
    activity: [],
    diff: null,
    tests: null,
    reviews,
    timeline: [],
  };
}

function event(
  id: number,
  review: ReviewEvidence,
): Extract<TaskEvent, { kind: "review.updated" }> {
  return {
    id,
    schema_version: 1,
    task_id: TASK_ID,
    kind: "review.updated",
    created_at: NOW,
    payload: { review },
  };
}

describe("projectReviewEvent", () => {
  it("treats the same task/round and canonical payload as an idempotent replay", () => {
    const existing = changesRequestedReview();
    const reordered = {
      ...structuredClone(existing),
      workspace_digest: {
        value: existing.workspace_digest.value,
        algorithm: existing.workspace_digest.algorithm,
      },
    };

    const result = projectReviewEvent(detail([existing]), event(11, reordered));

    expect(result.kind).toBe("replayed");
    if (result.kind !== "replayed") return;
    expect(result.detail.reviews).toEqual([existing]);
    expect(result.detail.event_cursor).toBe(11);
    expect(result.detail.task.last_event_id).toBe(11);
  });

  it("reports a protocol conflict for the same task/round with a different payload", () => {
    const existing = changesRequestedReview();
    const conflicting = { ...structuredClone(existing), summary: "Different payload." };

    const result = projectReviewEvent(detail([existing]), event(11, conflicting));

    expect(result).toEqual({
      kind: "conflict",
      reason: "review_payload_conflict",
    });
  });

  it("appends a valid next round without changing lifecycle or readiness", () => {
    const before = detail([changesRequestedReview()]);

    const result = projectReviewEvent(before, event(11, approvedReview()));

    expect(result.kind).toBe("applied");
    if (result.kind !== "applied") return;
    expect(result.detail.reviews.map((review) => review.round)).toEqual([1, 2]);
    expect(result.detail.task.status).toBe("running");
    expect(result.detail.task.delivery_readiness).toBe("unreviewed");
    expect(result.detail.event_cursor).toBe(11);
  });

  it("marks an incomplete round history stale while advancing its detail cursor", () => {
    const before = detail();

    const result = projectReviewEvent(before, event(11, approvedReview()));

    expect(result.kind).toBe("stale");
    if (result.kind !== "stale") return;
    expect(result.reason).toBe("review_history_incomplete");
    expect(result.detail.reviews).toEqual([]);
    expect(result.detail.event_cursor).toBe(11);
    expect(result.detail.task.last_event_id).toBe(11);
  });

  it("reports a protocol conflict when a complete history rejects the next-round delta", () => {
    const invalid = {
      ...approvedReview(),
      required_checks: [requiredCheck(), requiredCheck("unexpected-check")],
      added_required_checks: [],
    };

    const result = projectReviewEvent(
      detail([changesRequestedReview()]),
      event(11, invalid),
    );

    expect(result.kind).toBe("conflict");
    if (result.kind !== "conflict") return;
    expect(result.reason).toBe("review_history_conflict");
  });
});
