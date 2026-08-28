import { describe, expect, it } from "vitest";

import type {
  DeliveryMergeOperation,
  DeliveryOperation,
  DeliveryTask,
} from "./types";
import {
  validateDeliveryCommandResponse,
  validateDeliveryOperation,
  validateDeliveryTask,
} from "./deliveryValidation";
import { ValidationError } from "./validation";

const TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const OPERATION_ID = "22222222-2222-4222-8222-222222222222";
const MERGE_ID = "33333333-3333-4333-8333-333333333333";
const OID_A = "1".repeat(40);
const OID_B = "2".repeat(40);
const FINGERPRINT = "a".repeat(64);

const READY_MERGE: DeliveryMergeOperation = {
  operation_id: OPERATION_ID,
  version: 3,
  state: "preflight_ready",
  review_generation: 7,
  workspace_fingerprint: FINGERPRINT,
  candidate_source_tree: OID_B,
  preflight_source_commit: OID_B,
  source_commit: null,
  target_branch: "refs/heads/main",
  target_head: OID_A,
  conflicts: null,
  failure: null,
};

const TASK: DeliveryTask = {
  task_id: TASK_ID,
  eligibility: "eligible",
  reasons: [],
  evidence: {
    review_generation: 7,
    workspace_fingerprint: FINGERPRINT,
  },
  target: { available: true, branch: "refs/heads/main", head: OID_A },
  source: null,
  latest_merge: READY_MERGE,
  latest_cleanup: null,
  disposition: null,
  allowed_actions: ["accept_merge"],
};

function mergeOperation(
  overrides: Partial<DeliveryMergeOperation> = {},
): DeliveryOperation {
  return { kind: "merge", ...READY_MERGE, ...overrides };
}

function expectInvalid(invocation: () => unknown, path: string): void {
  try {
    invocation();
    throw new Error("expected validation to fail");
  } catch (error) {
    expect(error).toBeInstanceOf(ValidationError);
    expect(error).toMatchObject({ path });
  }
}

describe("delivery exact response validation", () => {
  it("accepts generated-schema-shaped task, operation, and command projections", () => {
    expect(validateDeliveryTask(structuredClone(TASK))).toEqual(TASK);
    const operation = mergeOperation();
    expect(validateDeliveryOperation(structuredClone(operation))).toEqual(operation);
    expect(
      validateDeliveryCommandResponse({ receipt: "created", operation }),
    ).toEqual({ receipt: "created", operation });

    const merged = structuredClone(TASK);
    merged.eligibility = "ineligible";
    merged.reasons = ["already_merged"];
    merged.allowed_actions = [];
    merged.source = {
      state: "committed",
      version: 3,
      source_ref: "refs/heads/coding-agent/task",
      source_oid: OID_B,
    };
    merged.latest_merge = {
      ...READY_MERGE,
      state: "merged",
      source_commit: OID_B,
    };
    merged.disposition = {
      merged_operation_id: OPERATION_ID,
      source_ref: "refs/heads/coding-agent/task",
      source_oid: OID_B,
      worktree: { state: "retained_locked", version: 1, failure: null },
      branch: { state: "retained", version: 1, failure: null },
    };
    merged.latest_cleanup = {
      operation_id: MERGE_ID,
      cleanup_kind: "remove_worktree",
      version: 1,
      state: "unlock_pending",
      expected_disposition_version: 1,
      expected_merge_operation_id: OPERATION_ID,
      expected_source_ref: "refs/heads/coding-agent/task",
      expected_source_oid: OID_B,
      target_branch: null,
      target_head: null,
      failure: null,
    };
    expect(validateDeliveryTask(merged)).toEqual(merged);
  });

  it("accepts a fresh remove retry for a retained unlocked worktree", () => {
    const retryable = structuredClone(TASK);
    retryable.eligibility = "ineligible";
    retryable.reasons = ["already_merged"];
    retryable.allowed_actions = ["remove_worktree"];
    retryable.source = {
      state: "committed",
      version: 3,
      source_ref: "refs/heads/coding-agent/task",
      source_oid: OID_B,
    };
    retryable.latest_merge = {
      ...READY_MERGE,
      state: "merged",
      source_commit: OID_B,
    };
    retryable.disposition = {
      merged_operation_id: OPERATION_ID,
      source_ref: "refs/heads/coding-agent/task",
      source_oid: OID_B,
      worktree: {
        state: "retained_unlocked",
        version: 2,
        failure: null,
      },
      branch: { state: "retained", version: 1, failure: null },
    };
    retryable.latest_cleanup = {
      operation_id: MERGE_ID,
      cleanup_kind: "remove_worktree",
      version: 2,
      state: "failed",
      expected_disposition_version: 2,
      expected_merge_operation_id: OPERATION_ID,
      expected_source_ref: "refs/heads/coding-agent/task",
      expected_source_oid: OID_B,
      target_branch: null,
      target_head: null,
      failure: { code: "TARGET_WORKTREE_DIRTY" },
    };

    expect(validateDeliveryTask(retryable)).toEqual(retryable);
  });

  it("rejects unknown, missing, and null required fields at their exact paths", () => {
    expectInvalid(
      () => validateDeliveryTask({ ...structuredClone(TASK), unknown: true }),
      "$.unknown",
    );
    const missing = structuredClone(TASK) as Record<string, unknown>;
    delete missing.target;
    expectInvalid(() => validateDeliveryTask(missing), "$.target");
    const missingCleanup = structuredClone(TASK) as Record<string, unknown>;
    delete missingCleanup.latest_cleanup;
    expectInvalid(
      () => validateDeliveryTask(missingCleanup),
      "$.latest_cleanup",
    );
    expectInvalid(
      () => validateDeliveryTask({ ...structuredClone(TASK), task_id: null }),
      "$.task_id",
    );
  });

  it("rejects unsafe integers and noncanonical UUID, OID, ref, and fingerprint values", () => {
    expectInvalid(
      () =>
        validateDeliveryTask({
          ...structuredClone(TASK),
          task_id: TASK_ID.toUpperCase(),
        }),
      "$.task_id",
    );
    expectInvalid(
      () =>
        validateDeliveryOperation(
          mergeOperation({ version: Number.MAX_SAFE_INTEGER + 1 }),
        ),
      "$.version",
    );
    expectInvalid(
      () => validateDeliveryOperation(mergeOperation({ target_head: "A".repeat(40) })),
      "$.target_head",
    );
    expectInvalid(
      () => validateDeliveryOperation(mergeOperation({ target_head: "0".repeat(40) })),
      "$.target_head",
    );
    expectInvalid(
      () => validateDeliveryOperation(mergeOperation({ target_branch: "main" })),
      "$.target_branch",
    );
    expectInvalid(
      () =>
        validateDeliveryOperation(
          mergeOperation({ workspace_fingerprint: FINGERPRINT.toUpperCase() }),
        ),
      "$.workspace_fingerprint",
    );
  });

  it("rejects illegal source, merge, cleanup, conflict, and failure combinations", () => {
    expectInvalid(
      () =>
        validateDeliveryTask({
          ...structuredClone(TASK),
          source: {
            state: "commit_pending",
            version: 1,
            source_ref: "refs/heads/coding-agent/task",
            source_oid: null,
          },
        }),
      "$.source.source_oid",
    );
    expectInvalid(
      () =>
        validateDeliveryOperation(
          mergeOperation({ candidate_source_tree: null, preflight_source_commit: null }),
        ),
      "$.candidate_source_tree",
    );
    expectInvalid(
      () => validateDeliveryOperation(mergeOperation({ state: "conflict" })),
      "$.conflicts",
    );
    expectInvalid(
      () =>
        validateDeliveryOperation({
          kind: "cleanup",
          operation_id: OPERATION_ID,
          cleanup_kind: "remove_worktree",
          version: 1,
          state: "delete_pending",
          expected_disposition_version: 1,
          expected_merge_operation_id: MERGE_ID,
          expected_source_ref: "refs/heads/coding-agent/task",
          expected_source_oid: OID_B,
          target_branch: null,
          target_head: null,
          failure: null,
        }),
      "$.state",
    );
  });

  it("enforces canonical conflict paths and aggregate payload bounds", () => {
    const conflict = mergeOperation({
      state: "conflict",
      conflicts: {
        path_count: 1,
        paths: [{ encoding: "utf8", path: "src/lib.rs" }],
        payload_bytes: new TextEncoder().encode("src/lib.rs").length,
        truncated: false,
      },
      failure: { code: "MERGE_CONFLICT" },
    });
    expect(validateDeliveryOperation(conflict)).toEqual(conflict);

    const oversizedPath = structuredClone(conflict);
    if (oversizedPath.kind === "merge" && oversizedPath.conflicts !== null) {
      oversizedPath.conflicts.paths[0] = {
        encoding: "utf8",
        path: "a".repeat(4_097),
      };
      oversizedPath.conflicts.payload_bytes = 4_097;
    }
    expectInvalid(
      () => validateDeliveryOperation(oversizedPath),
      "$.conflicts.paths[0].path",
    );

    const oversizedPayload = structuredClone(conflict);
    if (oversizedPayload.kind === "merge" && oversizedPayload.conflicts !== null) {
      oversizedPayload.conflicts.paths = Array.from({ length: 17 }, (_, index) => ({
        encoding: "utf8" as const,
        path: `${String(index).padStart(2, "0")}${"a".repeat(4_094)}`,
      }));
      oversizedPayload.conflicts.path_count = 17;
      oversizedPayload.conflicts.payload_bytes = 17 * 4_096;
    }
    expectInvalid(
      () => validateDeliveryOperation(oversizedPayload),
      "$.conflicts.payload_bytes",
    );

    const padded = structuredClone(conflict);
    if (padded.kind === "merge" && padded.conflicts !== null) {
      padded.conflicts.paths = [{ encoding: "base64url", path: "_w==" }];
      padded.conflicts.payload_bytes = 4;
    }
    expectInvalid(
      () => validateDeliveryOperation(padded),
      "$.conflicts.paths[0].path",
    );
  });
});
