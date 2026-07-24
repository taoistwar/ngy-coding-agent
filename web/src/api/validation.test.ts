import { describe, expect, it } from "vitest";

import {
  ValidationError,
  validateActivityEntry,
  validateBootstrapResponse,
  validatePlanSnapshot,
  validateReviewEvidence,
  validateTaskDetail,
  validateTaskEvent,
  validateTaskList,
} from "./validation";

const TASK_ID = "00000000-0000-4000-8000-000000000001";
const REPOSITORY_ID = "00000000-0000-4000-8000-000000000002";
const CLIENT_REQUEST_ID = "00000000-0000-4000-8000-000000000003";
const NOW = "2026-07-23T01:02:03.000000000Z";
const DIGEST = {
  algorithm: "workspace_fingerprint_v1" as const,
  value: "a".repeat(64),
};
const INITIAL_TEST = {
  id: "check-1",
  kind: "cargo_test" as const,
  package: "coding-agent-core",
  integration_test: null,
};
const REVIEWER_CHECK = {
  id: "check-2",
  kind: "cargo_check" as const,
  package: "coding-agent-core",
};

function passedEvidence(checkId: string, generation: number, roleRun = 1) {
  return {
    check_id: checkId,
    actor: "executor" as const,
    role_run: roleRun,
    workspace_generation: generation,
    workspace_digest: DIGEST,
    status: "passed" as const,
    duration_ms: 12,
    summary: "passed",
    truncated: false,
  };
}

function changesRequestedReview() {
  return {
    round: 1,
    decision_source: "reviewer" as const,
    workspace_generation: 1,
    workspace_digest: DIGEST,
    verdict: "changes_requested" as const,
    summary: "Please address the blocking issue",
    findings: [
      {
        id: "review-1-finding-1",
        severity: "blocking" as const,
        message: "The focused behavior is incomplete",
        path: "web/src/api/validation.ts",
        line: 10,
      },
    ],
    added_required_checks: [REVIEWER_CHECK],
    required_checks: [INITIAL_TEST, REVIEWER_CHECK],
    check_evidence: [],
    coverage: null,
    created_at: NOW,
  };
}

function approvedReview() {
  return {
    round: 2,
    decision_source: "reviewer" as const,
    workspace_generation: 2,
    workspace_digest: DIGEST,
    verdict: "approved" as const,
    summary: "All required checks passed",
    findings: [],
    added_required_checks: [],
    required_checks: [INITIAL_TEST, REVIEWER_CHECK],
    check_evidence: [
      passedEvidence(INITIAL_TEST.id, 2, 2),
      passedEvidence(REVIEWER_CHECK.id, 2, 2),
    ],
    coverage: {
      generation: 2,
      workspace_digest: DIGEST,
      manifest_sha256: "b".repeat(64),
      covered_chunks: [0, 1],
      total_chunks: 2,
    },
    created_at: NOW,
  };
}

function laterChangesRequestedReview(round: 2 | 3) {
  return {
    ...changesRequestedReview(),
    round,
    workspace_generation: round,
    summary: `Round ${round} still has a blocking issue`,
    findings: [
      {
        id: `review-${round}-finding-1`,
        severity: "blocking" as const,
        message: "The blocking issue remains",
        path: null,
        line: null,
      },
    ],
    added_required_checks: [],
  };
}

function validDetail() {
  return {
    task: {
      id: TASK_ID,
      client_request_id: CLIENT_REQUEST_ID,
      repository_id: REPOSITORY_ID,
      prompt: "Implement strict Web validation",
      status: "running" as const,
      delivery_readiness: "unreviewed" as const,
      attempt: 1,
      retry_of: null,
      created_at: NOW,
      started_at: NOW,
      finished_at: null,
      last_event_id: 12,
      failure: null,
    },
    plan: {
      format_version: 1,
      revision: 1,
      summary: "Implement the approved contract",
      items: [
        {
          id: "step-1",
          title: "Validate the API",
          description: "Reject malformed REST and SSE projections",
          acceptance_criteria: ["Focused Web tests pass"],
          status: "running" as const,
        },
      ],
      initial_required_checks: [INITIAL_TEST],
    },
    activity: [
      {
        id: "activity-1",
        level: "info" as const,
        actor: "executor" as const,
        role_run: 1,
        message: "Validating",
        created_at: NOW,
      },
    ],
    diff: null,
    tests: null,
    reviews: [changesRequestedReview(), approvedReview()],
    timeline: [],
    event_cursor: 12,
  };
}

function reviewEvent(review: unknown = approvedReview()) {
  return {
    id: 13,
    schema_version: 1,
    kind: "review.updated",
    task_id: TASK_ID,
    created_at: NOW,
    payload: { review },
  };
}

// Corruption cases intentionally cross the generated static types.
// Keeping the escape hatch in one test-only helper makes those mutations explicit.
function mutable(value: unknown): any {
  return structuredClone(value);
}

describe("shared wire validation", () => {
  it("accepts a complete two-round TaskDetail and the approved-before-lifecycle state", () => {
    const detail = validDetail();

    expect(validateTaskDetail(detail)).toBe(detail);
  });

  it("rejects extra fields, missing required-nullable fields, and wrong nulls", () => {
    const extra = mutable(validDetail()) as Record<string, unknown>;
    extra.extra = true;
    expect(() => validateTaskDetail(extra)).toThrow(ValidationError);

    const missingCoverage = mutable(validDetail());
    delete (missingCoverage.reviews[0] as Partial<typeof missingCoverage.reviews[0]>)
      .coverage;
    expect(() => validateTaskDetail(missingCoverage)).toThrow(/coverage/);

    const missingPackage = mutable(validDetail());
    delete (
      missingPackage.plan.initial_required_checks[0] as Partial<
        typeof missingPackage.plan.initial_required_checks[0]
      >
    ).package;
    expect(() => validateTaskDetail(missingPackage)).toThrow(/package/);

    const nullReviews = {
      ...validDetail(),
      reviews: null,
    };
    expect(() => validateTaskDetail(nullReviews)).toThrow(/reviews/);
  });

  it.each([
    ["unsafe generation", Number.MAX_SAFE_INTEGER + 1, "workspace_generation"],
    ["negative generation", -1, "workspace_generation"],
    ["fractional generation", 1.5, "workspace_generation"],
  ])("rejects %s", (_label, value, field) => {
    const detail = mutable(validDetail());
    Object.assign(detail.reviews[0], { [field]: value });

    expect(() => validateTaskDetail(detail)).toThrow(/workspace_generation/);
  });

  it("requires exact lowercase digests and valid RFC3339 UTC timestamps", () => {
    const uppercaseDigest = mutable(validDetail());
    uppercaseDigest.reviews[0].workspace_digest.value = "A".repeat(64);
    expect(() => validateTaskDetail(uppercaseDigest)).toThrow(/workspace_digest/);

    const offsetTime = mutable(validDetail());
    offsetTime.reviews[0].created_at = "2026-07-23T01:02:03+00:00";
    expect(() => validateTaskDetail(offsetTime)).toThrow(/created_at/);

    const impossibleTime = mutable(validDetail());
    impossibleTime.activity[0].created_at = "2026-02-30T01:02:03Z";
    expect(() => validateTaskDetail(impossibleTime)).toThrow(/created_at/);
  });

  it.each([
    ["leading dash", "-private"],
    ["path separator", "workspace/core"],
    ["more than 128 UTF-8 bytes", "é".repeat(65)],
  ])("rejects a non-canonical cargo selector with %s", (_label, selector) => {
    const detail = mutable(validDetail());
    detail.plan.initial_required_checks[0].package = selector;

    expect(() => validateTaskDetail(detail)).toThrow(/package/);
  });

  it("requires an integration-test selector to name a package", () => {
    const detail = mutable(validDetail());
    detail.plan.initial_required_checks[0].package = null;
    detail.plan.initial_required_checks[0].integration_test = "contract";

    expect(() => validateTaskDetail(detail)).toThrow(/integration_test/);
  });

  it("rejects duplicate check IDs, duplicate selectors, and duplicate evidence IDs", () => {
    const duplicateId = mutable(validDetail());
    duplicateId.reviews[0].required_checks.push({
      id: INITIAL_TEST.id,
      kind: "cargo_check",
      package: "other",
    });
    expect(() => validateTaskDetail(duplicateId)).toThrow(/duplicate.*id/i);

    const duplicateSelector = mutable(validDetail());
    duplicateSelector.reviews[0].required_checks.push({
      ...REVIEWER_CHECK,
      id: "check-3",
    });
    expect(() => validateTaskDetail(duplicateSelector)).toThrow(/selector/i);

    const duplicateEvidence = mutable(validDetail());
    duplicateEvidence.reviews[1].check_evidence.push(
      passedEvidence(INITIAL_TEST.id, 2, 2),
    );
    expect(() => validateTaskDetail(duplicateEvidence)).toThrow(/check_evidence/i);
  });

  it("validates the exact append-only added-check delta across rounds", () => {
    const wrongDelta = mutable(validDetail());
    wrongDelta.reviews[1].added_required_checks = [REVIEWER_CHECK];
    expect(() => validateTaskDetail(wrongDelta)).toThrow(/added_required_checks/);

    const missingRound = mutable(validDetail());
    missingRound.reviews[1].round = 3;
    expect(() => validateTaskDetail(missingRound)).toThrow(/round/);
  });

  it("binds terminal readiness to the latest review without rejecting the live gap", () => {
    const approved = mutable(validDetail());
    approved.task.status = "completed";
    approved.task.delivery_readiness = "review_approved";
    approved.task.finished_at = NOW;
    expect(validateTaskDetail(approved)).toBe(approved);

    const approvedWithoutFinalReview = mutable(approved);
    approvedWithoutFinalReview.reviews.pop();
    expect(() => validateTaskDetail(approvedWithoutFinalReview)).toThrow(
      /latest persisted review.*approved/,
    );

    const reviewAfterApproval = mutable(validDetail());
    reviewAfterApproval.reviews = [
      { ...approvedReview(), round: 1 },
      laterChangesRequestedReview(2),
    ];
    reviewAfterApproval.reviews[0].findings = [];
    reviewAfterApproval.reviews[0].added_required_checks = [REVIEWER_CHECK];
    expect(() => validateTaskDetail(reviewAfterApproval)).toThrow(
      /approved evidence.*final review/,
    );

    const rejected = mutable(validDetail());
    rejected.task.status = "failed";
    rejected.task.delivery_readiness = "review_rejected";
    rejected.task.finished_at = NOW;
    rejected.task.failure = {
      code: "REVIEW_REJECTED",
      message: "Review rejected after three rounds",
      retryable: true,
    };
    rejected.reviews = [
      changesRequestedReview(),
      laterChangesRequestedReview(2),
      laterChangesRequestedReview(3),
    ];
    expect(validateTaskDetail(rejected)).toBe(rejected);

    const rejectedAtRoundTwo = mutable(rejected);
    rejectedAtRoundTwo.reviews.pop();
    expect(() => validateTaskDetail(rejectedAtRoundTwo)).toThrow(
      /round 3 changes_requested/,
    );
  });

  it("keeps legacy unreviewed completion empty while preserving intermediate failures", () => {
    const historical = mutable(validDetail());
    historical.task.status = "completed";
    historical.task.finished_at = NOW;
    historical.reviews = [];
    expect(validateTaskDetail(historical)).toBe(historical);

    historical.reviews = [changesRequestedReview()];
    expect(() => validateTaskDetail(historical)).toThrow(
      /completed \+ unreviewed/,
    );

    const failedDuringRework = mutable(validDetail());
    failedDuringRework.task.status = "failed";
    failedDuringRework.task.finished_at = NOW;
    failedDuringRework.task.failure = {
      code: "EXECUTOR_FAILED",
      message: "Executor failed during rework",
      retryable: false,
    };
    failedDuringRework.reviews = [changesRequestedReview()];
    expect(validateTaskDetail(failedDuringRework)).toBe(failedDuringRework);

    const impossibleApprovedFailure = mutable(failedDuringRework);
    impossibleApprovedFailure.reviews = validDetail().reviews;
    expect(() => validateTaskDetail(impossibleApprovedFailure)).toThrow(
      /cannot retain an approved/,
    );
  });

  it("validates approved evidence and complete sorted coverage", () => {
    const missingEvidence = mutable(approvedReview());
    missingEvidence.check_evidence.pop();
    expect(() => validateReviewEvidence(missingEvidence)).toThrow(/check_evidence/);

    const incompleteCoverage = mutable(approvedReview());
    incompleteCoverage.coverage.covered_chunks = [1];
    expect(() => validateReviewEvidence(incompleteCoverage)).toThrow(/covered_chunks/);

    const duplicateChunk = mutable(approvedReview());
    duplicateChunk.coverage.covered_chunks = [0, 0];
    expect(() => validateReviewEvidence(duplicateChunk)).toThrow(/covered_chunks/);

    const blockingApproval = mutable(approvedReview());
    blockingApproval.findings = [
      {
        id: "review-2-finding-1",
        severity: "blocking",
        message: "blocking",
        path: null,
        line: null,
      },
    ];
    expect(() => validateReviewEvidence(blockingApproval)).toThrow(/approved/);
  });

  it("requires the exact system invalidation decision", () => {
    const system = {
      ...changesRequestedReview(),
      decision_source: "system",
      added_required_checks: [],
      required_checks: [INITIAL_TEST],
      findings: [
        {
          id: "review-1-finding-1",
          severity: "blocking",
          message:
            "Workspace changed during review; review evidence was invalidated",
          path: null,
          line: null,
        },
      ],
    };
    expect(validateReviewEvidence(system)).toBe(system);

    const forged = mutable(system);
    forged.findings[0].message = "Reviewer requested changes";
    expect(() => validateReviewEvidence(forged)).toThrow(/system/);
  });

  it("uses UTF-8 JSON bytes for evidence and retained summary limits", () => {
    const oversizedEvidence = mutable(changesRequestedReview());
    oversizedEvidence.findings = Array.from({ length: 32 }, (_, index) => ({
      id: `review-1-finding-${index + 1}`,
      severity: "blocking" as const,
      message: "🙂".repeat(2_048),
      path: null,
      line: null,
    }));
    expect(() => validateReviewEvidence(oversizedEvidence)).toThrow(/128 KiB/);

    const oversizedSummary = mutable(approvedReview());
    oversizedSummary.check_evidence[0].summary = "你".repeat(683);
    expect(() => validateReviewEvidence(oversizedSummary)).toThrow(/summary/);

    const invalidScalar = mutable(approvedReview());
    invalidScalar.summary = "\ud800";
    expect(() => validateReviewEvidence(invalidScalar)).toThrow(/surrogate/);
  });
});

describe("Plan and Activity compatibility validation", () => {
  it("accepts materialized v0 plans and rejects v0 structured fields", () => {
    const legacy = {
      format_version: 0,
      revision: 0,
      summary: "",
      items: [
        {
          id: "legacy-step",
          title: "Historical title",
          description: "",
          acceptance_criteria: [],
          status: "completed",
        },
      ],
      initial_required_checks: [],
    };

    expect(validatePlanSnapshot(legacy)).toBe(legacy);
    expect(() =>
      validatePlanSnapshot({ ...legacy, summary: "not recorded in v0" }),
    ).toThrow(/format_version 0/);
  });

  it("enforces v1 item/check constraints and the 64 KiB UTF-8 JSON limit", () => {
    const valid = validDetail().plan;
    expect(validatePlanSnapshot(valid)).toBe(valid);

    const noCargoTest = mutable(validDetail().plan);
    noCargoTest.initial_required_checks = [REVIEWER_CHECK];
    expect(() => validatePlanSnapshot(noCargoTest)).toThrow(/cargo_test/);

    const duplicateItem = mutable(validDetail().plan);
    duplicateItem.items.push({ ...duplicateItem.items[0] });
    expect(() => validatePlanSnapshot(duplicateItem)).toThrow(/duplicate.*id/i);

    const tooLarge = mutable(validDetail().plan);
    tooLarge.items = Array.from({ length: 32 }, (_, index) => ({
      id: `step-${index + 1}`,
      title: `Step ${index + 1}`,
      description: "🙂".repeat(600),
      acceptance_criteria: ["done"],
      status: "pending" as const,
    }));
    expect(() => validatePlanSnapshot(tooLarge)).toThrow(/64 KiB/);
  });

  it("binds system to null role_run and role actors to a positive role_run", () => {
    const system = {
      id: "activity-system",
      level: "warning",
      actor: "system",
      role_run: null,
      message: "Recovered",
      created_at: NOW,
    };
    expect(validateActivityEntry(system)).toBe(system);

    expect(() =>
      validateActivityEntry({ ...system, role_run: 1 }),
    ).toThrow(/role_run/);
    expect(() =>
      validateActivityEntry({
        ...system,
        actor: "reviewer",
        role_run: null,
      }),
    ).toThrow(/role_run/);
  });
});

describe("TaskEvent validation boundaries", () => {
  it("validates a review event only with self-contained rules", () => {
    const event = reviewEvent({
      ...approvedReview(),
      // A single event cannot prove the previous ledger. This is an ordered
      // subset of its own cumulative set and is therefore self-contained-valid.
      added_required_checks: [REVIEWER_CHECK],
    });

    expect(validateTaskEvent(event)).toBe(event);
  });

  it("rejects malformed exact envelopes and lifecycle identity mismatches", () => {
    const extra = {
      ...reviewEvent(),
      extra: true,
    };
    expect(() => validateTaskEvent(extra)).toThrow(/extra/);

    const queued = {
      id: 14,
      schema_version: 1,
      kind: "task.queued",
      task_id: TASK_ID,
      created_at: NOW,
      payload: {
        task: {
          ...validDetail().task,
          status: "queued",
          started_at: null,
          last_event_id: 13,
        },
      },
    };
    expect(() => validateTaskEvent(queued)).toThrow(/last_event_id/);
  });
});

describe("REST collection validation", () => {
  function bootstrap() {
    return {
      csrf_token: "csrf",
      repositories: [
        {
          id: REPOSITORY_ID,
          selected_path: "E:\\repo",
          display_name: "repo",
          git_root: "E:\\repo",
          cargo_workspace_root: "E:\\repo",
          created_at: NOW,
          last_opened_at: NOW,
        },
      ],
      tasks: [validDetail().task],
      latest_event_id: 12,
      server_started_at: NOW,
      service_state: "ready",
      service_state_generation: 1,
      max_concurrent_tasks: 2,
    };
  }

  it("requires unique IDs and bounds Task cursors by the bootstrap watermark", () => {
    const valid = bootstrap();
    expect(validateBootstrapResponse(valid)).toBe(valid);

    const duplicateTask = mutable(valid);
    duplicateTask.tasks.push({ ...duplicateTask.tasks[0] });
    expect(() => validateBootstrapResponse(duplicateTask)).toThrow(
      /duplicate task id/,
    );
    expect(() => validateTaskList(duplicateTask.tasks)).toThrow(
      /duplicate task id/,
    );

    const cursorBeyondWatermark = mutable(valid);
    cursorBeyondWatermark.latest_event_id = 11;
    expect(() => validateBootstrapResponse(cursorBeyondWatermark)).toThrow(
      /latest_event_id/,
    );

    const missingRepository = mutable(valid);
    missingRepository.tasks[0].repository_id =
      "00000000-0000-4000-8000-000000000099";
    expect(() => validateBootstrapResponse(missingRepository)).toThrow(
      /bootstrap repository/,
    );
  });
});
