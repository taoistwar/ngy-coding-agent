import type {
  DeliveryArtifactDisposition,
  DeliveryCleanupOperation,
  DeliveryCleanupOperationEnvelope,
  DeliveryCommandResponse,
  DeliveryConflictSummary,
  DeliveryMergeOperation,
  DeliveryMergeOperationEnvelope,
  DeliveryMergeState,
  DeliveryOperation,
  DeliveryTask,
} from "./types";
import { ValidationError } from "./validation";

const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const OID_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const FINGERPRINT_PATTERN = /^[0-9a-f]{64}$/;
const FAILURE_CODE_PATTERN = /^[A-Z][A-Z0-9_]*$/;
const BASE64URL_PATTERN = /^[A-Za-z0-9_-]+$/;
const ZERO_OIDS = new Set(["0".repeat(40), "0".repeat(64)]);
const encoder = new TextEncoder();

const ELIGIBILITIES = ["eligible", "ineligible", "unavailable"] as const;
const ELIGIBILITY_REASONS = [
  "task_not_completed",
  "review_not_approved",
  "approved_evidence_missing",
  "attempt_artifact_missing",
  "attempt_artifact_not_ready",
  "task_active",
  "process_cleanup_unproven",
  "target_branch_detached",
  "target_branch_mismatch",
  "target_head_changed",
  "target_worktree_dirty",
  "target_ignored_path_collision",
  "target_git_operation_in_progress",
  "unsafe_git_configuration",
  "unsupported_git_attributes",
  "source_already_in_target",
  "runtime_drift",
  "delivery_owned",
  "already_merged",
  "reconciliation_required",
  "repository_busy",
  "repository_unavailable",
  "store_unavailable",
  "runtime_observation_unavailable",
  "service_not_ready",
] as const;
const ALLOWED_ACTIONS = [
  "run_preflight",
  "accept_merge",
  "remove_worktree",
  "delete_branch",
] as const;
const TARGET_UNAVAILABLE_REASONS = [
  "detached",
  "branch_mismatch",
  "observation_unavailable",
  "repository_busy",
  "repository_poisoned",
  "service_not_ready",
] as const;
const SOURCE_STATES = [
  "object_pending",
  "commit_pending",
  "committed",
  "reconciliation_required",
] as const;
const MERGE_STATES = [
  "preflight_pending",
  "preflight_ready",
  "accepted",
  "merge_pending",
  "merged",
  "abort_pending",
  "conflict",
  "rejected",
  "stale",
  "superseded",
  "failed",
  "reconciliation_required",
] as const;
const CLEANUP_STATES = [
  "unlock_pending",
  "unlocked_pending_remove",
  "remove_pending",
  "delete_pending",
  "completed",
  "failed",
  "reconciliation_required",
] as const;
const WORKTREE_STATES = [
  "retained_locked",
  "retained_unlocked",
  "removed",
  "reconciliation_required",
] as const;
const BRANCH_STATES = ["retained", "deleted", "reconciliation_required"] as const;
const REJECTED_FAILURES = new Set([
  "TASK_NOT_MERGE_ELIGIBLE",
  "TARGET_BRANCH_DETACHED",
  "TARGET_BRANCH_MISMATCH",
  "TARGET_WORKTREE_DIRTY",
  "TARGET_IGNORED_PATH_COLLISION",
  "TARGET_GIT_OPERATION_IN_PROGRESS",
  "UNSAFE_GIT_CONFIGURATION",
  "UNSUPPORTED_GIT_ATTRIBUTES",
  "SOURCE_ALREADY_IN_TARGET",
]);
const STALE_FAILURES = new Set([
  "DELIVERY_EVIDENCE_STALE",
  "TARGET_BRANCH_MISMATCH",
  "TARGET_HEAD_CHANGED",
  "DELIVERY_SOURCE_CHANGED",
]);
const RECONCILIATION_FAILURES = new Set([
  "DELIVERY_RECONCILIATION_REQUIRED",
  "DELIVERY_SOURCE_INCONSISTENT",
  "PROCESS_TREE_CLEANUP_FAILED",
  "WORKTREE_IDENTITY_MISMATCH",
  "UNSAFE_GIT_CONFIGURATION",
  "UNSUPPORTED_GIT_ATTRIBUTES",
]);

export function validateDeliveryTask(value: unknown): DeliveryTask {
  const task = exactObject(value, "$", [
    "task_id",
    "eligibility",
    "reasons",
    "evidence",
    "target",
    "source",
    "latest_merge",
    "latest_cleanup",
    "disposition",
    "allowed_actions",
  ]);
  canonicalUuid(task.task_id, "$.task_id");
  const eligibility = enumValue(task.eligibility, "$.eligibility", ELIGIBILITIES);
  const reasons = boundedArray(
    task.reasons,
    "$.reasons",
    32,
    (item, path) => enumValue(item, path, ELIGIBILITY_REASONS),
  );
  unique(reasons, "$.reasons");
  if (task.evidence !== null) readEvidence(task.evidence, "$.evidence");
  readTarget(task.target, "$.target");
  if (task.source !== null) readSource(task.source, "$.source");
  if (task.latest_merge !== null) {
    readMergeOperation(task.latest_merge, "$.latest_merge", false);
  }
  if (task.latest_cleanup !== null) {
    readCleanupOperation(task.latest_cleanup, "$.latest_cleanup", false);
  }
  if (task.disposition !== null) {
    readDisposition(task.disposition, "$.disposition");
  }
  const actions = boundedArray(
    task.allowed_actions,
    "$.allowed_actions",
    4,
    (item, path) => enumValue(item, path, ALLOWED_ACTIONS),
  );
  unique(actions, "$.allowed_actions");
  validateTaskShape(
    task,
    eligibility,
    reasons,
    actions,
  );
  return value as DeliveryTask;
}

function validateTaskShape(
  task: Record<string, unknown>,
  eligibility: (typeof ELIGIBILITIES)[number],
  reasons: (typeof ELIGIBILITY_REASONS)[number][],
  actions: (typeof ALLOWED_ACTIONS)[number][],
): void {
  const projection = task as unknown as DeliveryTask;
  if ((eligibility === "eligible") !== (reasons.length === 0)) {
    fail(
      "$.reasons",
      "must be empty exactly when the delivery projection is eligible",
    );
  }
  if (
    eligibility === "eligible" &&
    (projection.evidence === null || projection.target.available !== true)
  ) {
    fail("$.eligibility", "eligible delivery requires evidence and an available target");
  }
  if (
    actions.some((action) => action === "run_preflight" || action === "accept_merge") &&
    eligibility !== "eligible"
  ) {
    fail("$.allowed_actions", "merge actions require eligible delivery");
  }
  if (
    actions.includes("accept_merge") &&
    projection.latest_merge?.state !== "preflight_ready"
  ) {
    fail("$.allowed_actions", "accept_merge requires a preflight_ready operation");
  }

  const disposition = projection.disposition;
  const source = projection.source;
  const merge = projection.latest_merge;
  const cleanup = projection.latest_cleanup;
  if (disposition !== null) {
    if (
      merge?.state !== "merged" ||
      merge.operation_id !== disposition.merged_operation_id
    ) {
      fail("$.disposition.merged_operation_id", "must identify the merged operation");
    }
    if (
      source?.state !== "committed" ||
      source.source_ref !== disposition.source_ref ||
      source.source_oid !== disposition.source_oid
    ) {
      fail("$.disposition.source_oid", "must match the committed delivery source");
    }
  } else if (merge?.state === "merged") {
    fail("$.disposition", "must be present atomically with a merged operation");
  }
  if (cleanup !== null) {
    if (disposition === null) {
      fail("$.latest_cleanup", "requires a persisted artifact disposition");
    }
    if (
      cleanup.expected_merge_operation_id !== disposition.merged_operation_id ||
      cleanup.expected_source_ref !== disposition.source_ref ||
      cleanup.expected_source_oid !== disposition.source_oid
    ) {
      fail(
        "$.latest_cleanup.expected_merge_operation_id",
        "must match the persisted artifact disposition",
      );
    }
    if (
      cleanup.state !== "completed" &&
      cleanup.state !== "failed" &&
      cleanup.state !== "reconciliation_required" &&
      actions.some(
        (action) => action === "remove_worktree" || action === "delete_branch",
      )
    ) {
      fail("$.allowed_actions", "cleanup actions require no pending cleanup operation");
    }
  }
  const worktreeRemovalCanBeRetried =
    disposition?.worktree.state === "retained_unlocked" &&
    cleanup?.cleanup_kind === "remove_worktree" &&
    cleanup.state === "failed";
  if (
    actions.includes("remove_worktree") &&
    disposition?.worktree.state !== "retained_locked" &&
    !worktreeRemovalCanBeRetried
  ) {
    fail(
      "$.allowed_actions",
      "remove_worktree requires a retained worktree or a failed remove retry",
    );
  }
  if (
    actions.includes("delete_branch") &&
    (disposition?.worktree.state !== "removed" ||
      disposition.branch.state !== "retained")
  ) {
    fail("$.allowed_actions", "delete_branch requires a removed worktree and retained branch");
  }
}

export function validateDeliveryOperation(value: unknown): DeliveryOperation {
  const record = recordValue(value, "$");
  if (record.kind === "merge") {
    return readMergeOperation(value, "$", true);
  }
  if (record.kind === "cleanup") {
    return readCleanupOperation(value, "$", true);
  }
  fail("$.kind", "must be one of merge, cleanup");
}

export function validateDeliveryCommandResponse(
  value: unknown,
): DeliveryCommandResponse {
  const response = exactObject(value, "$", ["receipt", "operation"]);
  enumValue(response.receipt, "$.receipt", ["created", "existing"] as const);
  validateDeliveryOperationAt(response.operation, "$.operation");
  return value as DeliveryCommandResponse;
}

function validateDeliveryOperationAt(value: unknown, path: string): DeliveryOperation {
  const record = recordValue(value, path);
  if (record.kind === "merge") return readMergeOperation(value, path, true);
  if (record.kind === "cleanup") return readCleanupOperation(value, path, true);
  fail(`${path}.kind`, "must be one of merge, cleanup");
}

function readEvidence(value: unknown, path: string): void {
  const evidence = exactObject(value, path, [
    "review_generation",
    "workspace_fingerprint",
  ]);
  nonNegativeSafeInteger(evidence.review_generation, `${path}.review_generation`);
  fingerprint(evidence.workspace_fingerprint, `${path}.workspace_fingerprint`);
}

function readTarget(value: unknown, path: string): void {
  const target = recordValue(value, path);
  if (target.available === true) {
    const available = exactObject(value, path, ["available", "branch", "head"]);
    branchRef(available.branch, `${path}.branch`);
    gitOid(available.head, `${path}.head`);
    return;
  }
  if (target.available === false) {
    const unavailable = exactObject(value, path, ["available", "reason"]);
    enumValue(
      unavailable.reason,
      `${path}.reason`,
      TARGET_UNAVAILABLE_REASONS,
    );
    return;
  }
  fail(`${path}.available`, "must be the boolean literal true or false");
}

function readSource(value: unknown, path: string): void {
  const source = exactObject(value, path, [
    "state",
    "version",
    "source_ref",
    "source_oid",
  ]);
  const state = enumValue(source.state, `${path}.state`, SOURCE_STATES);
  positiveSafeInteger(source.version, `${path}.version`);
  branchRef(source.source_ref, `${path}.source_ref`);
  const sourceOid = nullableOid(source.source_oid, `${path}.source_oid`);
  if (state === "object_pending" && sourceOid !== null) {
    fail(`${path}.source_oid`, "must be null while source is object_pending");
  }
  if ((state === "commit_pending" || state === "committed") && sourceOid === null) {
    fail(`${path}.source_oid`, `must be present while source is ${state}`);
  }
}

function readMergeOperation(
  value: unknown,
  path: string,
  envelope: true,
): DeliveryMergeOperationEnvelope;
function readMergeOperation(
  value: unknown,
  path: string,
  envelope: false,
): DeliveryMergeOperation;
function readMergeOperation(
  value: unknown,
  path: string,
  envelope: boolean,
): DeliveryMergeOperation | DeliveryMergeOperationEnvelope {
  const fields = [
    ...(envelope ? ["kind"] : []),
    "operation_id",
    "version",
    "state",
    "review_generation",
    "workspace_fingerprint",
    "candidate_source_tree",
    "preflight_source_commit",
    "source_commit",
    "target_branch",
    "target_head",
    "conflicts",
    "failure",
  ];
  const operation = exactObject(value, path, fields);
  if (envelope) enumValue(operation.kind, `${path}.kind`, ["merge"] as const);
  canonicalUuid(operation.operation_id, `${path}.operation_id`);
  const version = positiveSafeInteger(operation.version, `${path}.version`);
  const state = enumValue(operation.state, `${path}.state`, MERGE_STATES);
  nonNegativeSafeInteger(operation.review_generation, `${path}.review_generation`);
  fingerprint(operation.workspace_fingerprint, `${path}.workspace_fingerprint`);
  const candidate = nullableOid(
    operation.candidate_source_tree,
    `${path}.candidate_source_tree`,
  );
  const preflightSource = nullableOid(
    operation.preflight_source_commit,
    `${path}.preflight_source_commit`,
  );
  const source = nullableOid(operation.source_commit, `${path}.source_commit`);
  branchRef(operation.target_branch, `${path}.target_branch`);
  const targetHead = gitOid(operation.target_head, `${path}.target_head`);

  if ((candidate === null) !== (preflightSource === null)) {
    fail(
      `${path}.preflight_source_commit`,
      "must be present exactly when candidate_source_tree is present",
    );
  }
  for (const [name, oid] of [
    ["candidate_source_tree", candidate],
    ["preflight_source_commit", preflightSource],
    ["source_commit", source],
  ] as const) {
    if (oid !== null && oid.length !== targetHead.length) {
      fail(`${path}.${name}`, "must use the target object format");
    }
  }
  validateMergeStateShape(operation, path, state, version, candidate, source);
  return value as DeliveryMergeOperation | DeliveryMergeOperationEnvelope;
}

function validateMergeStateShape(
  operation: Record<string, unknown>,
  path: string,
  state: DeliveryMergeState,
  version: number,
  candidate: string | null,
  source: string | null,
): void {
  const unboundAllowed =
    state === "preflight_pending" ||
    state === "rejected" ||
    state === "stale" ||
    state === "reconciliation_required";
  if (candidate === null && !unboundAllowed) {
    fail(
      `${path}.candidate_source_tree`,
      `must be present while merge is ${state}`,
    );
  }
  if (state === "preflight_pending") {
    if ((candidate === null && version !== 1) || (candidate !== null && version < 2)) {
      fail(`${path}.version`, "is inconsistent with preflight_pending inputs");
    }
  }
  const earlyWithoutSource = new Set<DeliveryMergeState>([
    "preflight_pending",
    "preflight_ready",
    "rejected",
    "stale",
    "superseded",
  ]);
  if (earlyWithoutSource.has(state) && source !== null) {
    fail(`${path}.source_commit`, `must be null while merge is ${state}`);
  }
  if (
    (state === "merge_pending" ||
      state === "merged" ||
      state === "abort_pending" ||
      state === "failed") &&
    source === null
  ) {
    fail(`${path}.source_commit`, `must be present while merge is ${state}`);
  }

  const conflicts =
    operation.conflicts === null
      ? null
      : readConflictSummary(operation.conflicts, `${path}.conflicts`);
  if ((state === "conflict" || state === "abort_pending") && conflicts === null) {
    fail(`${path}.conflicts`, `must be present while merge is ${state}`);
  }
  if (
    conflicts !== null &&
    state !== "conflict" &&
    state !== "abort_pending" &&
    state !== "reconciliation_required"
  ) {
    fail(`${path}.conflicts`, `must be null while merge is ${state}`);
  }
  if (state === "abort_pending" && conflicts?.path_count === 0) {
    fail(`${path}.conflicts.path_count`, "must be positive while abort is pending");
  }

  const failure =
    operation.failure === null
      ? null
      : readFailure(operation.failure, `${path}.failure`);
  const failureAllowed = (() => {
    switch (state) {
      case "conflict":
        return failure === "MERGE_CONFLICT";
      case "rejected":
        return failure !== null && REJECTED_FAILURES.has(failure);
      case "stale":
        return failure !== null && STALE_FAILURES.has(failure);
      case "failed":
        return (
          failure !== null &&
          (failure === "TARGET_HEAD_CHANGED" ||
            failure === "COMMAND_TIMED_OUT" ||
            REJECTED_FAILURES.has(failure))
        );
      case "reconciliation_required":
        return failure !== null && RECONCILIATION_FAILURES.has(failure);
      default:
        return failure === null;
    }
  })();
  if (!failureAllowed) {
    fail(`${path}.failure`, `is inconsistent with merge state ${state}`);
  }
}

function readCleanupOperation(
  value: unknown,
  path: string,
  envelope: true,
): DeliveryCleanupOperationEnvelope;
function readCleanupOperation(
  value: unknown,
  path: string,
  envelope: false,
): DeliveryCleanupOperation;
function readCleanupOperation(
  value: unknown,
  path: string,
  envelope: boolean,
): DeliveryCleanupOperation | DeliveryCleanupOperationEnvelope {
  const operation = exactObject(value, path, [
    ...(envelope ? ["kind"] : []),
    "operation_id",
    "cleanup_kind",
    "version",
    "state",
    "expected_disposition_version",
    "expected_merge_operation_id",
    "expected_source_ref",
    "expected_source_oid",
    "target_branch",
    "target_head",
    "failure",
  ]);
  if (envelope) enumValue(operation.kind, `${path}.kind`, ["cleanup"] as const);
  canonicalUuid(operation.operation_id, `${path}.operation_id`);
  const kind = enumValue(operation.cleanup_kind, `${path}.cleanup_kind`, [
    "remove_worktree",
    "delete_branch",
  ] as const);
  positiveSafeInteger(operation.version, `${path}.version`);
  const state = enumValue(operation.state, `${path}.state`, CLEANUP_STATES);
  positiveSafeInteger(
    operation.expected_disposition_version,
    `${path}.expected_disposition_version`,
  );
  canonicalUuid(
    operation.expected_merge_operation_id,
    `${path}.expected_merge_operation_id`,
  );
  branchRef(operation.expected_source_ref, `${path}.expected_source_ref`);
  const sourceOid = gitOid(
    operation.expected_source_oid,
    `${path}.expected_source_oid`,
  );
  const targetBranch = nullableBranchRef(operation.target_branch, `${path}.target_branch`);
  const targetHead = nullableOid(operation.target_head, `${path}.target_head`);
  const failure =
    operation.failure === null
      ? null
      : readFailure(operation.failure, `${path}.failure`);

  const removeStates = new Set([
    "unlock_pending",
    "unlocked_pending_remove",
    "remove_pending",
    "completed",
    "failed",
    "reconciliation_required",
  ]);
  const deleteStates = new Set([
    "delete_pending",
    "completed",
    "failed",
    "reconciliation_required",
  ]);
  if (kind === "remove_worktree") {
    if (!removeStates.has(state)) {
      fail(`${path}.state`, `is invalid for ${kind}`);
    }
    if (targetBranch !== null || targetHead !== null) {
      fail(`${path}.target_branch`, "cleanup worktree target fields must be null");
    }
  } else {
    if (!deleteStates.has(state)) {
      fail(`${path}.state`, `is invalid for ${kind}`);
    }
    if (targetBranch === null || targetHead === null) {
      fail(`${path}.target_branch`, "branch cleanup target fields must be present");
    }
    if (targetHead.length !== sourceOid.length) {
      fail(`${path}.target_head`, "must use the source object format");
    }
  }
  if ((state === "failed" || state === "reconciliation_required") !== (failure !== null)) {
    fail(`${path}.failure`, `is inconsistent with cleanup state ${state}`);
  }
  return value as DeliveryCleanupOperation | DeliveryCleanupOperationEnvelope;
}

function readConflictSummary(value: unknown, path: string): DeliveryConflictSummary {
  const summary = exactObject(value, path, [
    "path_count",
    "paths",
    "payload_bytes",
    "truncated",
  ]);
  const pathCount = integerBetween(summary.path_count, `${path}.path_count`, 0, 128);
  const paths = boundedArray(summary.paths, `${path}.paths`, 128, (item, itemPath) => {
    const conflictPath = exactObject(item, itemPath, ["encoding", "path"]);
    const encoding = enumValue(conflictPath.encoding, `${itemPath}.encoding`, [
      "utf8",
      "base64url",
    ] as const);
    const wirePath = nonEmptyString(conflictPath.path, `${itemPath}.path`);
    const wireBytes = encoder.encode(wirePath);
    if (wireBytes.length > 4_096) {
      fail(`${itemPath}.path`, "must contain at most 4096 UTF-8 bytes");
    }
    if (encoding === "utf8") {
      canonicalRelativePath(wireBytes, `${itemPath}.path`);
    } else {
      const raw = canonicalBase64Url(wirePath, `${itemPath}.path`);
      try {
        new TextDecoder("utf-8", { fatal: true }).decode(raw);
      } catch {
        canonicalRelativePath(raw, `${itemPath}.path`);
        return { raw, wireBytes: wireBytes.length };
      }
      fail(`${itemPath}.path`, "base64url is only valid for non-UTF-8 paths");
    }
    return { raw: wireBytes, wireBytes: wireBytes.length };
  });
  const payloadBytes = integerBetween(
    summary.payload_bytes,
    `${path}.payload_bytes`,
    0,
    65_536,
  );
  const truncated = booleanValue(summary.truncated, `${path}.truncated`);
  if (pathCount < paths.length || (!truncated && pathCount !== paths.length)) {
    fail(`${path}.path_count`, "must describe the exact or truncated path collection");
  }
  const observedPayload = paths.reduce((sum, item) => sum + item.wireBytes, 0);
  if (payloadBytes !== observedPayload) {
    fail(`${path}.payload_bytes`, "must equal the encoded path payload size");
  }
  const identities = paths.map(({ raw }) => bytesKey(raw));
  unique(identities, `${path}.paths`);
  return value as DeliveryConflictSummary;
}

function readDisposition(value: unknown, path: string): DeliveryArtifactDisposition {
  const disposition = exactObject(value, path, [
    "merged_operation_id",
    "source_ref",
    "source_oid",
    "worktree",
    "branch",
  ]);
  canonicalUuid(disposition.merged_operation_id, `${path}.merged_operation_id`);
  branchRef(disposition.source_ref, `${path}.source_ref`);
  gitOid(disposition.source_oid, `${path}.source_oid`);
  readDispositionState(
    disposition.worktree,
    `${path}.worktree`,
    WORKTREE_STATES,
  );
  readDispositionState(disposition.branch, `${path}.branch`, BRANCH_STATES);
  return value as DeliveryArtifactDisposition;
}

function readDispositionState(
  value: unknown,
  path: string,
  states: readonly string[],
): void {
  const disposition = exactObject(value, path, ["state", "version", "failure"]);
  const state = enumValue(disposition.state, `${path}.state`, states);
  positiveSafeInteger(disposition.version, `${path}.version`);
  const failure =
    disposition.failure === null
      ? null
      : readFailure(disposition.failure, `${path}.failure`);
  if ((state === "reconciliation_required") !== (failure !== null)) {
    fail(`${path}.failure`, `is inconsistent with disposition state ${state}`);
  }
}

function readFailure(value: unknown, path: string): string {
  const failure = exactObject(value, path, ["code"]);
  const code = nonEmptyString(failure.code, `${path}.code`);
  if (code.length > 128 || !FAILURE_CODE_PATTERN.test(code)) {
    fail(`${path}.code`, "must be a canonical bounded stable failure code");
  }
  return code;
}

function exactObject(
  value: unknown,
  path: string,
  expectedKeys: readonly string[],
): Record<string, unknown> {
  const record = recordValue(value, path);
  const expected = new Set(expectedKeys);
  for (const key of expectedKeys) {
    if (!Object.prototype.hasOwnProperty.call(record, key)) {
      fail(`${path}.${key}`, "missing required field");
    }
  }
  for (const key of Object.keys(record)) {
    if (!expected.has(key)) {
      fail(`${path}.${key}`, "extra field is not allowed");
    }
  }
  return record;
}

function recordValue(value: unknown, path: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    fail(path, "must be an object");
  }
  return value as Record<string, unknown>;
}

function boundedArray<T>(
  value: unknown,
  path: string,
  maximum: number,
  read: (item: unknown, path: string) => T,
): T[] {
  if (!Array.isArray(value)) fail(path, "must be an array");
  if (value.length > maximum) fail(path, `must contain at most ${maximum} items`);
  return value.map((item, index) => read(item, `${path}[${index}]`));
}

function enumValue<const T>(value: unknown, path: string, allowed: readonly T[]): T {
  if (!allowed.some((candidate) => Object.is(candidate, value))) {
    fail(path, `must be one of ${allowed.map(String).join(", ")}`);
  }
  return value as T;
}

function canonicalUuid(value: unknown, path: string): string {
  const uuid = nonEmptyString(value, path);
  if (!UUID_PATTERN.test(uuid) || uuid === "00000000-0000-0000-0000-000000000000") {
    fail(path, "must be a canonical non-nil UUID");
  }
  return uuid;
}

function gitOid(value: unknown, path: string): string {
  const oid = nonEmptyString(value, path);
  if (!OID_PATTERN.test(oid) || ZERO_OIDS.has(oid)) {
    fail(path, "must be a canonical non-zero Git object ID");
  }
  return oid;
}

function nullableOid(value: unknown, path: string): string | null {
  return value === null ? null : gitOid(value, path);
}

function fingerprint(value: unknown, path: string): string {
  const candidate = nonEmptyString(value, path);
  if (!FINGERPRINT_PATTERN.test(candidate)) {
    fail(path, "must be a canonical SHA-256 fingerprint");
  }
  return candidate;
}

function branchRef(value: unknown, path: string): string {
  const branch = nonEmptyString(value, path);
  const short = branch.startsWith("refs/heads/")
    ? branch.slice("refs/heads/".length)
    : null;
  if (
    short === null ||
    short.length === 0 ||
    encoder.encode(branch).length > 4_096 ||
    short.startsWith("-") ||
    branch.endsWith("/") ||
    branch.endsWith(".") ||
    branch.includes("..") ||
    branch.includes("@{") ||
    branch.includes("//") ||
    Array.from(branch).some(invalidRefCharacter) ||
    short
      .split("/")
      .some(
        (component) =>
          component.length === 0 ||
          component.startsWith(".") ||
          component.endsWith(".lock"),
      )
  ) {
    fail(path, "must be a canonical bounded refs/heads branch");
  }
  return branch;
}

function invalidRefCharacter(character: string): boolean {
  const codePoint = character.codePointAt(0) ?? 0;
  return (
    codePoint <= 0x1f ||
    (codePoint >= 0x7f && codePoint <= 0x9f) ||
    character === " " ||
    "~^:?*[\\".includes(character)
  );
}

function nullableBranchRef(value: unknown, path: string): string | null {
  return value === null ? null : branchRef(value, path);
}

function nonEmptyString(value: unknown, path: string): string {
  if (typeof value !== "string" || value.length === 0) {
    fail(path, "must be a non-empty string");
  }
  return value;
}

function positiveSafeInteger(value: unknown, path: string): number {
  return integerBetween(value, path, 1, MAX_SAFE_INTEGER);
}

function nonNegativeSafeInteger(value: unknown, path: string): number {
  return integerBetween(value, path, 0, MAX_SAFE_INTEGER);
}

function integerBetween(
  value: unknown,
  path: string,
  minimum: number,
  maximum: number,
): number {
  if (!Number.isSafeInteger(value) || Number(value) < minimum || Number(value) > maximum) {
    fail(path, `must be a safe integer in ${minimum}..${maximum}`);
  }
  return Number(value);
}

function booleanValue(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") fail(path, "must be a boolean");
  return value;
}

function canonicalBase64Url(value: string, path: string): Uint8Array {
  if (!BASE64URL_PATTERN.test(value) || value.includes("=")) {
    fail(path, "must be unpadded base64url");
  }
  const padding = "=".repeat((4 - (value.length % 4)) % 4);
  let decoded: string;
  try {
    decoded = atob(value.replaceAll("-", "+").replaceAll("_", "/") + padding);
  } catch {
    fail(path, "must be canonical base64url");
  }
  const bytes = Uint8Array.from(decoded, (character) => character.charCodeAt(0));
  if (base64Url(bytes) !== value) fail(path, "must be canonical base64url");
  return bytes;
}

function base64Url(value: Uint8Array): string {
  let binary = "";
  for (const byte of value) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

function canonicalRelativePath(value: Uint8Array, path: string): void {
  if (value.length === 0 || value[0] === 47 || value.includes(0)) {
    fail(path, "must be a non-empty canonical relative path");
  }
  const components: number[][] = [[]];
  for (const byte of value) {
    if (byte === 47) components.push([]);
    else components.at(-1)?.push(byte);
  }
  if (
    components.some(
      (component) =>
        component.length === 0 ||
        (component.length === 1 && component[0] === 46) ||
        (component.length === 2 && component[0] === 46 && component[1] === 46),
    )
  ) {
    fail(path, "must be a canonical relative path");
  }
}

function bytesKey(value: Uint8Array): string {
  return Array.from(value, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function unique(values: readonly string[], path: string): void {
  if (new Set(values).size !== values.length) fail(path, "must not contain duplicates");
}

function fail(path: string, message: string): never {
  throw new ValidationError(path, message);
}
