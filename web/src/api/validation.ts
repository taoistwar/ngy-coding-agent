import type {
  ActivityEntry,
  BootstrapResponse,
  CancellationAcceptedResponse,
  PlanSnapshot,
  QuitResponse,
  Repository,
  RequiredCheck,
  ReviewEvidence,
  Task,
  TaskDetail,
  TaskEvent,
} from "./types";

const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;
const MAX_U32 = 0xffff_ffff;
const MAX_PLAN_BYTES = 64 * 1024;
const MAX_PLAN_ITEMS = 32;
const MAX_PLAN_SUMMARY_SCALARS = 4_096;
const MAX_PLAN_TITLE_SCALARS = 256;
const MAX_PLAN_DESCRIPTION_SCALARS = 4_096;
const MAX_ACCEPTANCE_CRITERIA = 8;
const MAX_ACCEPTANCE_CRITERION_SCALARS = 1_024;
const MAX_REQUIRED_CHECKS = 16;
const MAX_CARGO_SELECTOR_BYTES = 128;
const MAX_REVIEW_EVIDENCE_BYTES = 128 * 1024;
const MAX_REVIEW_SUMMARY_SCALARS = 4_096;
const MAX_FINDINGS = 32;
const MAX_FINDING_MESSAGE_SCALARS = 2_048;
const MAX_CHECK_SUMMARY_BYTES = 2_048;
const MAX_REVIEW_CHUNKS = 8;
const SYSTEM_WORKSPACE_CHANGED_MESSAGE =
  "Workspace changed during review; review evidence was invalidated";

const TASK_EVENT_KINDS = [
  "task.queued",
  "task.started",
  "plan.updated",
  "activity.appended",
  "diff.updated",
  "test.updated",
  "review.updated",
  "task.completed",
  "task.failed",
  "task.cancelled",
  "task.interrupted",
] as const;

const TASK_EVENT_KIND_SET = new Set<string>(TASK_EVENT_KINDS);
const encoder = new TextEncoder();

export class ValidationError extends Error {
  readonly path: string;

  constructor(path: string, message: string) {
    super(`${path}: ${message}`);
    this.name = "ValidationError";
    this.path = path;
  }
}

export function validateTask(value: unknown): Task {
  return readTask(value, "$");
}

export function validatePlanSnapshot(value: unknown): PlanSnapshot {
  return readPlanSnapshot(value, "$");
}

export function validateActivityEntry(value: unknown): ActivityEntry {
  return readActivityEntry(value, "$");
}

export function validateReviewEvidence(value: unknown): ReviewEvidence {
  return readReviewEvidence(value, "$");
}

export function validateTaskEvent(value: unknown): TaskEvent {
  return readTaskEvent(value, "$");
}

export function validateTaskDetail(value: unknown): TaskDetail {
  const detail = exactObject(
    value,
    "$",
    [
      "task",
      "plan",
      "activity",
      "diff",
      "tests",
      "reviews",
      "timeline",
      "event_cursor",
    ],
  );
  const task = readTask(detail.task, "$.task");
  const plan =
    detail.plan === null
      ? null
      : readPlanSnapshot(detail.plan, "$.plan");
  const activity = readArray(detail.activity, "$.activity", readActivityEntry);
  assertUniqueBy(activity, ({ id }) => id, "$.activity", "activity id");
  const diff =
    detail.diff === null ? null : readDiffSnapshot(detail.diff, "$.diff");
  const tests =
    detail.tests === null ? null : readTestSnapshot(detail.tests, "$.tests");
  const reviews = readBoundedArray(
    detail.reviews,
    "$.reviews",
    readReviewEvidence,
    0,
    3,
  );
  const timeline = readArray(detail.timeline, "$.timeline", readTimelineEntry);
  const eventCursor = nonNegativeSafeInteger(
    detail.event_cursor,
    "$.event_cursor",
  );

  if (task.last_event_id > eventCursor) {
    fail(
      "$.event_cursor",
      "must be at least the Task last_event_id high watermark",
    );
  }
  for (let index = 1; index < timeline.length; index += 1) {
    const previous = timeline[index - 1];
    const current = timeline[index];
    if (
      previous === undefined ||
      current === undefined ||
      current.event_id <= previous.event_id
    ) {
      fail("$.timeline", "event_id values must be strictly increasing");
    }
  }
  if (
    timeline.some(({ event_id }) => event_id > eventCursor)
  ) {
    fail("$.timeline", "cannot contain an event beyond event_cursor");
  }

  if (reviews.length > 0 && (plan === null || plan.format_version !== 1)) {
    fail(
      "$.reviews",
      "review history requires a structured format_version 1 plan",
    );
  }
  let previousChecks: readonly RequiredCheck[] =
    plan?.format_version === 1 ? plan.initial_required_checks : [];
  for (const [index, review] of reviews.entries()) {
    const expectedRound = index + 1;
    if (review.round !== expectedRound) {
      fail(
        `$.reviews[${index}].round`,
        `must be the contiguous round ${expectedRound}`,
      );
    }
    if (
      review.required_checks.length !==
        previousChecks.length + review.added_required_checks.length ||
      !startsWithChecks(review.required_checks, previousChecks) ||
      !equalCheckArrays(
        review.required_checks.slice(previousChecks.length),
        review.added_required_checks,
      )
    ) {
      fail(
        `$.reviews[${index}].added_required_checks`,
        "must be the exact ordered append-only delta from the previous ledger",
      );
    }
    previousChecks = review.required_checks;
  }
  const latestReview = reviews.at(-1);
  if (
    reviews
      .slice(0, -1)
      .some(({ verdict }) => verdict === "approved")
  ) {
    fail("$.reviews", "approved evidence must be the final review in history");
  }
  if (
    task.delivery_readiness === "review_approved" &&
    latestReview?.verdict !== "approved"
  ) {
    fail(
      "$.reviews",
      "review_approved requires the latest persisted review to be approved",
    );
  }
  if (
    task.delivery_readiness === "review_rejected" &&
    !(
      reviews.length === 3 &&
      latestReview?.round === 3 &&
      latestReview.verdict === "changes_requested"
    )
  ) {
    fail(
      "$.reviews",
      "review_rejected requires round 3 changes_requested as the latest review",
    );
  }
  if (
    task.status === "completed" &&
    task.delivery_readiness === "unreviewed" &&
    reviews.length !== 0
  ) {
    fail(
      "$.reviews",
      "historical completed + unreviewed tasks must not carry review evidence",
    );
  }
  if (task.status === "queued" && reviews.length !== 0) {
    fail("$.reviews", "queued tasks must not carry review evidence");
  }
  if (
    task.delivery_readiness === "unreviewed" &&
    (task.status === "failed" ||
      task.status === "cancelled" ||
      task.status === "interrupted") &&
    latestReview?.verdict === "approved"
  ) {
    fail(
      "$.reviews",
      "an unreviewed terminal task cannot retain an approved final review",
    );
  }

  return value as TaskDetail;
}

export function validateBootstrapResponse(value: unknown): BootstrapResponse {
  const bootstrap = exactObject(
    value,
    "$",
    [
      "csrf_token",
      "repositories",
      "tasks",
      "latest_event_id",
      "server_started_at",
      "service_state",
      "service_state_generation",
      "max_concurrent_tasks",
    ],
  );
  nonEmptyString(bootstrap.csrf_token, "$.csrf_token");
  const repositories = readArray(
    bootstrap.repositories,
    "$.repositories",
    readRepository,
  );
  const tasks = readArray(bootstrap.tasks, "$.tasks", readTask);
  assertUniqueBy(
    repositories,
    ({ id }) => id,
    "$.repositories",
    "repository id",
  );
  assertUniqueBy(tasks, ({ id }) => id, "$.tasks", "task id");
  const latestEventId = nonNegativeSafeInteger(
    bootstrap.latest_event_id,
    "$.latest_event_id",
  );
  const repositoryIds = new Set(repositories.map(({ id }) => id));
  for (const [index, task] of tasks.entries()) {
    if (task.last_event_id > latestEventId) {
      fail(
        `$.tasks[${index}].last_event_id`,
        "must not exceed bootstrap latest_event_id",
      );
    }
    if (!repositoryIds.has(task.repository_id)) {
      fail(
        `$.tasks[${index}].repository_id`,
        "must reference a bootstrap repository",
      );
    }
  }
  utcTimestamp(bootstrap.server_started_at, "$.server_started_at");
  enumValue(
    bootstrap.service_state,
    "$.service_state",
    ["ready", "store_degraded", "quiescing"],
  );
  nonNegativeSafeInteger(
    bootstrap.service_state_generation,
    "$.service_state_generation",
  );
  positiveIntegerAtMost(
    bootstrap.max_concurrent_tasks,
    "$.max_concurrent_tasks",
    MAX_U32,
  );
  return value as BootstrapResponse;
}

export function validateRepository(value: unknown): Repository {
  return readRepository(value, "$");
}

export function validateRepositoryList(value: unknown): Repository[] {
  const repositories = readArray(value, "$", readRepository);
  assertUniqueBy(repositories, ({ id }) => id, "$", "repository id");
  return repositories;
}

export function validateTaskList(value: unknown): Task[] {
  const tasks = readArray(value, "$", readTask);
  assertUniqueBy(tasks, ({ id }) => id, "$", "task id");
  return tasks;
}

export function validateTaskEventList(value: unknown): TaskEvent[] {
  const events = readArray(value, "$", readTaskEvent);
  for (let index = 1; index < events.length; index += 1) {
    const previous = events[index - 1];
    const current = events[index];
    if (
      previous === undefined ||
      current === undefined ||
      current.id <= previous.id
    ) {
      fail("$", "TaskEvent id values must be strictly increasing");
    }
  }
  return events;
}

export function validateCancellationResponse(
  value: unknown,
): Task | CancellationAcceptedResponse {
  if (
    isRecord(value) &&
    Object.prototype.hasOwnProperty.call(value, "cancellation_requested")
  ) {
    const accepted = exactObject(
      value,
      "$",
      ["task", "cancellation_requested"],
    );
    readTask(accepted.task, "$.task");
    booleanValue(
      accepted.cancellation_requested,
      "$.cancellation_requested",
    );
    return value as CancellationAcceptedResponse;
  }
  return readTask(value, "$");
}

export function validateQuitResponse(value: unknown): QuitResponse {
  const response = exactObject(value, "$", ["status"]);
  if (response.status !== "shutting_down") {
    fail("$.status", 'must be "shutting_down"');
  }
  return value as QuitResponse;
}

function readRepository(value: unknown, path: string): Repository {
  const repository = exactObject(
    value,
    path,
    [
      "id",
      "selected_path",
      "display_name",
      "git_root",
      "cargo_workspace_root",
      "created_at",
      "last_opened_at",
    ],
  );
  uuid(repository.id, `${path}.id`);
  nonEmptyString(repository.selected_path, `${path}.selected_path`);
  nonEmptyString(repository.display_name, `${path}.display_name`);
  nonEmptyString(repository.git_root, `${path}.git_root`);
  nonEmptyString(
    repository.cargo_workspace_root,
    `${path}.cargo_workspace_root`,
  );
  utcTimestamp(repository.created_at, `${path}.created_at`);
  utcTimestamp(repository.last_opened_at, `${path}.last_opened_at`);
  return value as Repository;
}

function readTask(value: unknown, path: string): Task {
  const task = exactObject(
    value,
    path,
    [
      "id",
      "client_request_id",
      "repository_id",
      "prompt",
      "status",
      "delivery_readiness",
      "attempt",
      "retry_of",
      "created_at",
      "started_at",
      "finished_at",
      "last_event_id",
      "failure",
    ],
  );
  uuid(task.id, `${path}.id`);
  uuid(task.client_request_id, `${path}.client_request_id`);
  uuid(task.repository_id, `${path}.repository_id`);
  const prompt = boundedScalarString(task.prompt, `${path}.prompt`, 1, 50_000);
  if (prompt.trim() !== prompt) {
    fail(`${path}.prompt`, "must already be trimmed");
  }
  const status = enumValue(
    task.status,
    `${path}.status`,
    ["queued", "running", "completed", "failed", "cancelled", "interrupted"],
  );
  const readiness = enumValue(
    task.delivery_readiness,
    `${path}.delivery_readiness`,
    ["unreviewed", "review_approved", "review_rejected"],
  );
  positiveIntegerAtMost(task.attempt, `${path}.attempt`, MAX_U32);
  nullable(task.retry_of, `${path}.retry_of`, uuid);
  utcTimestamp(task.created_at, `${path}.created_at`);
  const startedAt = nullable(
    task.started_at,
    `${path}.started_at`,
    utcTimestamp,
  );
  const finishedAt = nullable(
    task.finished_at,
    `${path}.finished_at`,
    utcTimestamp,
  );
  positiveSafeInteger(task.last_event_id, `${path}.last_event_id`);
  const failureValue =
    task.failure === null
      ? null
      : readTaskFailure(task.failure, `${path}.failure`);

  const validState =
    (status === "queued" &&
      startedAt === null &&
      finishedAt === null &&
      failureValue === null) ||
    (status === "running" &&
      startedAt !== null &&
      finishedAt === null &&
      failureValue === null) ||
    (status === "completed" &&
      startedAt !== null &&
      finishedAt !== null &&
      failureValue === null) ||
    (status === "failed" &&
      startedAt !== null &&
      finishedAt !== null &&
      failureValue !== null) ||
    (status === "cancelled" &&
      finishedAt !== null &&
      failureValue === null) ||
    (status === "interrupted" &&
      finishedAt !== null &&
      failureValue !== null);
  if (!validState) {
    fail(path, `timestamp/failure fields do not match status ${status}`);
  }
  if (readiness === "review_approved" && status !== "completed") {
    fail(
      `${path}.delivery_readiness`,
      "review_approved requires completed status",
    );
  }
  if (
    readiness === "review_rejected" &&
    !(
      status === "failed" &&
      failureValue?.code === "REVIEW_REJECTED" &&
      failureValue.retryable
    )
  ) {
    fail(
      `${path}.delivery_readiness`,
      "review_rejected requires retryable REVIEW_REJECTED failure",
    );
  }
  return value as Task;
}

function readTaskFailure(
  value: unknown,
  path: string,
): NonNullable<Task["failure"]> {
  const failure = exactObject(value, path, ["code", "message", "retryable"]);
  stringValue(failure.code, `${path}.code`);
  stringValue(failure.message, `${path}.message`);
  booleanValue(failure.retryable, `${path}.retryable`);
  return value as NonNullable<Task["failure"]>;
}

function readPlanSnapshot(value: unknown, path: string): PlanSnapshot {
  const plan = exactObject(
    value,
    path,
    [
      "format_version",
      "revision",
      "summary",
      "items",
      "initial_required_checks",
    ],
  );
  const formatVersion = enumValue(
    plan.format_version,
    `${path}.format_version`,
    [0, 1],
  );
  const revision = nonNegativeSafeInteger(plan.revision, `${path}.revision`);
  const summary = stringValue(plan.summary, `${path}.summary`);
  if (!Array.isArray(plan.items)) {
    fail(`${path}.items`, "must be an array");
  }
  if (!Array.isArray(plan.initial_required_checks)) {
    fail(`${path}.initial_required_checks`, "must be an array");
  }

  if (formatVersion === 0) {
    if (summary !== "" || plan.initial_required_checks.length !== 0) {
      fail(
        path,
        "format_version 0 requires empty summary and initial_required_checks",
      );
    }
    for (const [index, itemValue] of plan.items.entries()) {
      const itemPath = `${path}.items[${index}]`;
      const item = exactObject(
        itemValue,
        itemPath,
        ["id", "title", "description", "acceptance_criteria", "status"],
      );
      stringValue(item.id, `${itemPath}.id`);
      stringValue(item.title, `${itemPath}.title`);
      if (item.description !== "") {
        fail(
          `${itemPath}.description`,
          "format_version 0 requires the safe empty value",
        );
      }
      if (
        !Array.isArray(item.acceptance_criteria) ||
        item.acceptance_criteria.length !== 0
      ) {
        fail(
          `${itemPath}.acceptance_criteria`,
          "format_version 0 requires the safe empty value",
        );
      }
      enumValue(
        item.status,
        `${itemPath}.status`,
        ["pending", "running", "completed"],
      );
    }
    return value as PlanSnapshot;
  }

  if (revision === 0) {
    fail(`${path}.revision`, "format_version 1 requires a positive revision");
  }
  boundedScalarString(
    summary,
    `${path}.summary`,
    0,
    MAX_PLAN_SUMMARY_SCALARS,
  );
  if (
    plan.items.length < 1 ||
    plan.items.length > MAX_PLAN_ITEMS
  ) {
    fail(`${path}.items`, `must contain 1..${MAX_PLAN_ITEMS} items`);
  }
  const itemIds = new Set<string>();
  let runningItems = 0;
  for (const [index, itemValue] of plan.items.entries()) {
    const itemPath = `${path}.items[${index}]`;
    const item = exactObject(
      itemValue,
      itemPath,
      ["id", "title", "description", "acceptance_criteria", "status"],
    );
    const id = nonEmptyString(item.id, `${itemPath}.id`);
    if (itemIds.has(id)) {
      fail(`${itemPath}.id`, `duplicate plan item id ${JSON.stringify(id)}`);
    }
    itemIds.add(id);
    boundedScalarString(
      item.title,
      `${itemPath}.title`,
      1,
      MAX_PLAN_TITLE_SCALARS,
    );
    boundedScalarString(
      item.description,
      `${itemPath}.description`,
      0,
      MAX_PLAN_DESCRIPTION_SCALARS,
    );
    const criteria = readBoundedArray(
      item.acceptance_criteria,
      `${itemPath}.acceptance_criteria`,
      (criterion, criterionPath) =>
        boundedScalarString(
          criterion,
          criterionPath,
          1,
          MAX_ACCEPTANCE_CRITERION_SCALARS,
        ),
      1,
      MAX_ACCEPTANCE_CRITERIA,
    );
    void criteria;
    const status = enumValue(
      item.status,
      `${itemPath}.status`,
      ["pending", "running", "completed"],
    );
    if (status === "running") {
      runningItems += 1;
    }
  }
  if (runningItems > 1) {
    fail(`${path}.items`, "must contain at most one running item");
  }
  readRequiredCheckArray(
    plan.initial_required_checks,
    `${path}.initial_required_checks`,
    1,
    MAX_REQUIRED_CHECKS,
    true,
  );
  encodedSizeAtMost(value, path, MAX_PLAN_BYTES, "64 KiB");
  return value as PlanSnapshot;
}

function readActivityEntry(value: unknown, path: string): ActivityEntry {
  const entry = exactObject(
    value,
    path,
    ["id", "level", "actor", "role_run", "message", "created_at"],
  );
  nonEmptyString(entry.id, `${path}.id`);
  enumValue(entry.level, `${path}.level`, ["info", "warning", "error"]);
  const actor = enumValue(
    entry.actor,
    `${path}.actor`,
    ["system", "planner", "executor", "reviewer"],
  );
  stringValue(entry.message, `${path}.message`);
  utcTimestamp(entry.created_at, `${path}.created_at`);
  if (actor === "system") {
    if (entry.role_run !== null) {
      fail(`${path}.role_run`, "system activity requires null");
    }
  } else {
    positiveSafeInteger(entry.role_run, `${path}.role_run`);
  }
  return value as ActivityEntry;
}

function readReviewEvidence(value: unknown, path: string): ReviewEvidence {
  const review = exactObject(
    value,
    path,
    [
      "round",
      "decision_source",
      "workspace_generation",
      "workspace_digest",
      "verdict",
      "summary",
      "findings",
      "added_required_checks",
      "required_checks",
      "check_evidence",
      "coverage",
      "created_at",
    ],
  );
  const round = integerBetween(review.round, `${path}.round`, 1, 3);
  const decisionSource = enumValue(
    review.decision_source,
    `${path}.decision_source`,
    ["reviewer", "system"],
  );
  const generation = nonNegativeSafeInteger(
    review.workspace_generation,
    `${path}.workspace_generation`,
  );
  const digest = readWorkspaceDigest(
    review.workspace_digest,
    `${path}.workspace_digest`,
  );
  const verdict = enumValue(
    review.verdict,
    `${path}.verdict`,
    ["approved", "changes_requested"],
  );
  boundedScalarString(
    review.summary,
    `${path}.summary`,
    1,
    MAX_REVIEW_SUMMARY_SCALARS,
  );
  const findings = readBoundedArray(
    review.findings,
    `${path}.findings`,
    (finding, findingPath, index) =>
      readReviewFinding(finding, findingPath, round, index + 1),
    0,
    MAX_FINDINGS,
  );
  const addedChecks = readRequiredCheckArray(
    review.added_required_checks,
    `${path}.added_required_checks`,
    0,
    MAX_REQUIRED_CHECKS,
    false,
  );
  const requiredChecks = readRequiredCheckArray(
    review.required_checks,
    `${path}.required_checks`,
    1,
    MAX_REQUIRED_CHECKS,
    true,
  );
  if (!isOrderedCheckSubset(addedChecks, requiredChecks)) {
    fail(
      `${path}.added_required_checks`,
      "must be an ordered subset of required_checks",
    );
  }
  const evidence = readBoundedArray(
    review.check_evidence,
    `${path}.check_evidence`,
    readCheckEvidence,
    0,
    MAX_REQUIRED_CHECKS,
  );
  const requiredIndex = new Map(
    requiredChecks.map((check, index) => [check.id, index] as const),
  );
  const evidenceIds = new Set<string>();
  let previousEvidenceIndex = -1;
  for (const [index, observation] of evidence.entries()) {
    const evidencePath = `${path}.check_evidence[${index}]`;
    if (evidenceIds.has(observation.check_id)) {
      fail(
        `${evidencePath}.check_id`,
        "duplicate check_evidence check_id",
      );
    }
    evidenceIds.add(observation.check_id);
    const checkIndex = requiredIndex.get(observation.check_id);
    if (checkIndex === undefined) {
      fail(
        `${evidencePath}.check_id`,
        "must reference required_checks",
      );
    }
    if (checkIndex <= previousEvidenceIndex) {
      fail(
        `${path}.check_evidence`,
        "must follow required_checks order",
      );
    }
    previousEvidenceIndex = checkIndex;
    if (observation.workspace_generation !== generation) {
      fail(
        `${evidencePath}.workspace_generation`,
        "must equal the parent review generation",
      );
    }
    if (!equalDigest(observation.workspace_digest, digest)) {
      fail(
        `${evidencePath}.workspace_digest`,
        "must equal the parent review digest",
      );
    }
  }

  const coverage =
    review.coverage === null
      ? null
      : readReviewCoverage(review.coverage, `${path}.coverage`);
  if (
    coverage !== null &&
    (coverage.generation !== generation ||
      !equalDigest(coverage.workspace_digest, digest))
  ) {
    fail(
      `${path}.coverage`,
      "generation and workspace_digest must equal the parent review",
    );
  }
  utcTimestamp(review.created_at, `${path}.created_at`);

  const blocking = findings.filter(
    ({ severity }) => severity === "blocking",
  );
  if (verdict === "approved") {
    if (decisionSource !== "reviewer") {
      fail(`${path}.verdict`, "approved must be a reviewer decision");
    }
    if (blocking.length > 0) {
      fail(`${path}.findings`, "approved cannot contain blocking findings");
    }
    if (
      coverage === null ||
      coverage.covered_chunks.length !== coverage.total_chunks ||
      !coverage.covered_chunks.every((chunk, index) => chunk === index)
    ) {
      fail(
        `${path}.coverage.covered_chunks`,
        "approved requires complete coverage of every chunk",
      );
    }
    if (
      evidence.length !== requiredChecks.length ||
      evidence.some(({ status }) => status !== "passed")
    ) {
      fail(
        `${path}.check_evidence`,
        "approved requires one current passed observation per required check",
      );
    }
  } else if (blocking.length === 0) {
    fail(
      `${path}.findings`,
      "changes_requested requires at least one blocking finding",
    );
  }

  if (decisionSource === "system") {
    const finding = findings[0];
    if (
      verdict !== "changes_requested" ||
      findings.length !== 1 ||
      finding === undefined ||
      finding.id !== `review-${round}-finding-1` ||
      finding.severity !== "blocking" ||
      finding.message !== SYSTEM_WORKSPACE_CHANGED_MESSAGE ||
      finding.path !== null ||
      finding.line !== null ||
      coverage !== null ||
      evidence.length !== 0
    ) {
      fail(
        path,
        "system decision must be the exact workspace-invalidation evidence",
      );
    }
  }

  encodedSizeAtMost(
    value,
    path,
    MAX_REVIEW_EVIDENCE_BYTES,
    "128 KiB",
  );
  return value as ReviewEvidence;
}

function readRequiredCheckArray(
  value: unknown,
  path: string,
  minimum: number,
  maximum: number,
  requireCargoTest: boolean,
): RequiredCheck[] {
  const checks = readBoundedArray(
    value,
    path,
    readRequiredCheck,
    minimum,
    maximum,
  );
  const ids = new Set<string>();
  const selectors = new Set<string>();
  for (const [index, check] of checks.entries()) {
    const checkPath = `${path}[${index}]`;
    if (ids.has(check.id)) {
      fail(`${checkPath}.id`, `duplicate check id ${JSON.stringify(check.id)}`);
    }
    ids.add(check.id);
    const selector = selectorKey(check);
    if (selectors.has(selector)) {
      fail(checkPath, "duplicate canonical selector");
    }
    selectors.add(selector);
  }
  if (requireCargoTest && !checks.some(({ kind }) => kind === "cargo_test")) {
    fail(path, "must contain at least one cargo_test");
  }
  return checks;
}

function readRequiredCheck(value: unknown, path: string): RequiredCheck {
  if (!isRecord(value)) {
    fail(path, "must be an object");
  }
  if (value.kind === "cargo_check") {
    const check = exactObject(
      value,
      path,
      ["id", "kind", "package"],
    );
    nonEmptyString(check.id, `${path}.id`);
    readSelector(check.package, `${path}.package`);
    return value as RequiredCheck;
  }
  if (value.kind === "cargo_test") {
    const check = exactObject(
      value,
      path,
      ["id", "kind", "package", "integration_test"],
    );
    nonEmptyString(check.id, `${path}.id`);
    const packageName = readSelector(check.package, `${path}.package`);
    const integrationTest = readSelector(
      check.integration_test,
      `${path}.integration_test`,
    );
    if (integrationTest !== null && packageName === null) {
      fail(
        `${path}.integration_test`,
        "requires a non-null package selector",
      );
    }
    return value as RequiredCheck;
  }
  fail(`${path}.kind`, 'must be "cargo_check" or "cargo_test"');
}

function readSelector(value: unknown, path: string): string | null {
  if (value === null) {
    return null;
  }
  const selector = stringValue(value, path);
  const bytes = encoder.encode(selector);
  if (
    bytes.length < 1 ||
    bytes.length > MAX_CARGO_SELECTOR_BYTES ||
    !/^[A-Za-z0-9_][A-Za-z0-9_-]*$/.test(selector)
  ) {
    fail(path, "must be a canonical Cargo selector of at most 128 UTF-8 bytes");
  }
  return selector;
}

function readCheckEvidence(
  value: unknown,
  path: string,
): ReviewEvidence["check_evidence"][number] {
  const evidence = exactObject(
    value,
    path,
    [
      "check_id",
      "actor",
      "role_run",
      "workspace_generation",
      "workspace_digest",
      "status",
      "duration_ms",
      "summary",
      "truncated",
    ],
  );
  nonEmptyString(evidence.check_id, `${path}.check_id`);
  enumValue(evidence.actor, `${path}.actor`, ["executor", "reviewer"]);
  positiveSafeInteger(evidence.role_run, `${path}.role_run`);
  nonNegativeSafeInteger(
    evidence.workspace_generation,
    `${path}.workspace_generation`,
  );
  readWorkspaceDigest(evidence.workspace_digest, `${path}.workspace_digest`);
  enumValue(
    evidence.status,
    `${path}.status`,
    ["passed", "failed", "cancelled"],
  );
  nonNegativeSafeInteger(evidence.duration_ms, `${path}.duration_ms`);
  boundedUtf8String(
    evidence.summary,
    `${path}.summary`,
    1,
    MAX_CHECK_SUMMARY_BYTES,
  );
  booleanValue(evidence.truncated, `${path}.truncated`);
  return value as ReviewEvidence["check_evidence"][number];
}

function readReviewFinding(
  value: unknown,
  path: string,
  round: number,
  ordinal: number,
): ReviewEvidence["findings"][number] {
  const finding = exactObject(
    value,
    path,
    ["id", "severity", "message", "path", "line"],
  );
  const expectedId = `review-${round}-finding-${ordinal}`;
  if (finding.id !== expectedId) {
    fail(`${path}.id`, `must be ${JSON.stringify(expectedId)}`);
  }
  enumValue(finding.severity, `${path}.severity`, ["blocking", "advisory"]);
  boundedScalarString(
    finding.message,
    `${path}.message`,
    1,
    MAX_FINDING_MESSAGE_SCALARS,
  );
  const reviewPath =
    finding.path === null
      ? null
      : readReviewPath(finding.path, `${path}.path`);
  const line =
    finding.line === null
      ? null
      : positiveSafeInteger(finding.line, `${path}.line`);
  if (line !== null && reviewPath === null) {
    fail(`${path}.line`, "requires a non-null path");
  }
  return value as ReviewEvidence["findings"][number];
}

function readReviewCoverage(
  value: unknown,
  path: string,
): NonNullable<ReviewEvidence["coverage"]> {
  const coverage = exactObject(
    value,
    path,
    [
      "generation",
      "workspace_digest",
      "manifest_sha256",
      "covered_chunks",
      "total_chunks",
    ],
  );
  nonNegativeSafeInteger(coverage.generation, `${path}.generation`);
  readWorkspaceDigest(coverage.workspace_digest, `${path}.workspace_digest`);
  lowerHex64(coverage.manifest_sha256, `${path}.manifest_sha256`);
  const chunks = readBoundedArray(
    coverage.covered_chunks,
    `${path}.covered_chunks`,
    (chunk, chunkPath) => integerBetween(chunk, chunkPath, 0, 7),
    0,
    MAX_REVIEW_CHUNKS,
  );
  const totalChunks = integerBetween(
    coverage.total_chunks,
    `${path}.total_chunks`,
    0,
    MAX_REVIEW_CHUNKS,
  );
  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    const previous = index === 0 ? undefined : chunks[index - 1];
    if (
      chunk === undefined ||
      chunk >= totalChunks ||
      (previous !== undefined && chunk <= previous)
    ) {
      fail(
        `${path}.covered_chunks`,
        "must be sorted, unique, and each chunk must be below total_chunks",
      );
    }
  }
  return value as NonNullable<ReviewEvidence["coverage"]>;
}

function readWorkspaceDigest(
  value: unknown,
  path: string,
): ReviewEvidence["workspace_digest"] {
  const digest = exactObject(value, path, ["algorithm", "value"]);
  if (digest.algorithm !== "workspace_fingerprint_v1") {
    fail(`${path}.algorithm`, 'must be "workspace_fingerprint_v1"');
  }
  lowerHex64(digest.value, `${path}.value`);
  return value as ReviewEvidence["workspace_digest"];
}

function readTaskEvent(value: unknown, path: string): TaskEvent {
  const event = exactObject(
    value,
    path,
    ["id", "schema_version", "kind", "task_id", "created_at", "payload"],
  );
  const id = positiveSafeInteger(event.id, `${path}.id`);
  if (event.schema_version !== 1) {
    fail(`${path}.schema_version`, "must equal 1");
  }
  const kind = stringValue(event.kind, `${path}.kind`);
  if (!TASK_EVENT_KIND_SET.has(kind)) {
    fail(`${path}.kind`, "is not a supported TaskEvent kind");
  }
  const taskId = uuid(event.task_id, `${path}.task_id`);
  const eventCreatedAt = utcTimestamp(event.created_at, `${path}.created_at`);

  switch (kind) {
    case "task.queued":
      readLifecyclePayload(event.payload, `${path}.payload`, taskId, id, "queued");
      break;
    case "task.started":
      readLifecyclePayload(event.payload, `${path}.payload`, taskId, id, "running");
      break;
    case "task.completed":
      readLifecyclePayload(
        event.payload,
        `${path}.payload`,
        taskId,
        id,
        "completed",
      );
      break;
    case "task.failed":
      readLifecyclePayload(event.payload, `${path}.payload`, taskId, id, "failed");
      break;
    case "task.cancelled":
      readLifecyclePayload(
        event.payload,
        `${path}.payload`,
        taskId,
        id,
        "cancelled",
      );
      break;
    case "task.interrupted":
      readLifecyclePayload(
        event.payload,
        `${path}.payload`,
        taskId,
        id,
        "interrupted",
      );
      break;
    case "plan.updated": {
      const payload = exactObject(event.payload, `${path}.payload`, ["plan"]);
      readPlanSnapshot(payload.plan, `${path}.payload.plan`);
      break;
    }
    case "activity.appended": {
      const payload = exactObject(event.payload, `${path}.payload`, ["entry"]);
      readActivityEntry(payload.entry, `${path}.payload.entry`);
      break;
    }
    case "diff.updated": {
      const payload = exactObject(event.payload, `${path}.payload`, ["diff"]);
      readDiffSnapshot(payload.diff, `${path}.payload.diff`);
      break;
    }
    case "test.updated": {
      const payload = exactObject(event.payload, `${path}.payload`, ["tests"]);
      readTestSnapshot(payload.tests, `${path}.payload.tests`);
      break;
    }
    case "review.updated": {
      const payload = exactObject(event.payload, `${path}.payload`, ["review"]);
      const review = readReviewEvidence(
        payload.review,
        `${path}.payload.review`,
      );
      if (review.created_at !== eventCreatedAt) {
        fail(
          `${path}.payload.review.created_at`,
          "must equal the review.updated event created_at",
        );
      }
      break;
    }
    default:
      fail(`${path}.kind`, "is not a supported TaskEvent kind");
  }
  return value as TaskEvent;
}

function readLifecyclePayload(
  value: unknown,
  path: string,
  taskId: string,
  eventId: number,
  expectedStatus: Task["status"],
): void {
  const payload = exactObject(value, path, ["task"]);
  const task = readTask(payload.task, `${path}.task`);
  if (task.id !== taskId) {
    fail(`${path}.task.id`, "must equal the event task_id");
  }
  if (task.last_event_id !== eventId) {
    fail(`${path}.task.last_event_id`, "must equal the event id");
  }
  if (task.status !== expectedStatus) {
    fail(
      `${path}.task.status`,
      `must be ${JSON.stringify(expectedStatus)} for this lifecycle event`,
    );
  }
}

function readDiffSnapshot(
  value: unknown,
  path: string,
): NonNullable<TaskDetail["diff"]> {
  const diff = exactObject(value, path, ["revision", "files"]);
  nonNegativeSafeInteger(diff.revision, `${path}.revision`);
  const files = readArray(diff.files, `${path}.files`, (fileValue, filePath) => {
    const file = exactObject(
      fileValue,
      filePath,
      ["path", "status", "patch", "additions", "deletions", "truncated"],
    );
    nonEmptyString(file.path, `${filePath}.path`);
    enumValue(
      file.status,
      `${filePath}.status`,
      ["added", "modified", "deleted"],
    );
    stringValue(file.patch, `${filePath}.patch`);
    nonNegativeSafeInteger(file.additions, `${filePath}.additions`);
    nonNegativeSafeInteger(file.deletions, `${filePath}.deletions`);
    booleanValue(file.truncated, `${filePath}.truncated`);
    return fileValue as NonNullable<TaskDetail["diff"]>["files"][number];
  });
  assertUniqueBy(files, ({ path: filePath }) => filePath, `${path}.files`, "path");
  return value as NonNullable<TaskDetail["diff"]>;
}

function readTestSnapshot(
  value: unknown,
  path: string,
): NonNullable<TaskDetail["tests"]> {
  const tests = exactObject(value, path, ["revision", "status", "cases"]);
  nonNegativeSafeInteger(tests.revision, `${path}.revision`);
  enumValue(
    tests.status,
    `${path}.status`,
    ["queued", "running", "passed", "failed", "cancelled"],
  );
  const cases = readArray(tests.cases, `${path}.cases`, (caseValue, casePath) => {
    const testCase = exactObject(
      caseValue,
      casePath,
      ["id", "name", "status", "duration_ms", "summary"],
    );
    nonEmptyString(testCase.id, `${casePath}.id`);
    nonEmptyString(testCase.name, `${casePath}.name`);
    enumValue(
      testCase.status,
      `${casePath}.status`,
      ["queued", "running", "passed", "failed", "cancelled"],
    );
    nonNegativeSafeInteger(testCase.duration_ms, `${casePath}.duration_ms`);
    stringValue(testCase.summary, `${casePath}.summary`);
    return caseValue as NonNullable<TaskDetail["tests"]>["cases"][number];
  });
  assertUniqueBy(cases, ({ id }) => id, `${path}.cases`, "test case id");
  return value as NonNullable<TaskDetail["tests"]>;
}

function readTimelineEntry(
  value: unknown,
  path: string,
): TaskDetail["timeline"][number] {
  const entry = exactObject(
    value,
    path,
    ["event_id", "kind", "label", "created_at", "failure"],
  );
  positiveSafeInteger(entry.event_id, `${path}.event_id`);
  const kind = stringValue(entry.kind, `${path}.kind`);
  if (!TASK_EVENT_KIND_SET.has(kind)) {
    fail(`${path}.kind`, "is not a supported TaskEvent kind");
  }
  stringValue(entry.label, `${path}.label`);
  utcTimestamp(entry.created_at, `${path}.created_at`);
  if (entry.failure !== null) {
    readTaskFailure(entry.failure, `${path}.failure`);
  }
  return value as TaskDetail["timeline"][number];
}

function exactObject(
  value: unknown,
  path: string,
  expectedKeys: readonly string[],
): Record<string, unknown> {
  if (!isRecord(value)) {
    fail(path, "must be an object");
  }
  const expected = new Set(expectedKeys);
  for (const key of expectedKeys) {
    if (!Object.prototype.hasOwnProperty.call(value, key)) {
      fail(`${path}.${key}`, "missing required field");
    }
  }
  for (const key of Object.keys(value)) {
    if (!expected.has(key)) {
      fail(`${path}.${key}`, "extra field is not allowed");
    }
  }
  return value;
}

function readArray<T>(
  value: unknown,
  path: string,
  read: (item: unknown, itemPath: string, index: number) => T,
): T[] {
  if (!Array.isArray(value)) {
    fail(path, "must be an array");
  }
  return value.map((item, index) => read(item, `${path}[${index}]`, index));
}

function readBoundedArray<T>(
  value: unknown,
  path: string,
  read: (item: unknown, itemPath: string, index: number) => T,
  minimum: number,
  maximum: number,
): T[] {
  if (!Array.isArray(value)) {
    fail(path, "must be an array");
  }
  if (value.length < minimum || value.length > maximum) {
    fail(path, `must contain ${minimum}..${maximum} items`);
  }
  return value.map((item, index) => read(item, `${path}[${index}]`, index));
}

function stringValue(value: unknown, path: string): string {
  if (typeof value !== "string") {
    fail(path, "must be a string");
  }
  if (!isUnicodeScalarString(value)) {
    fail(path, "must not contain an unpaired UTF-16 surrogate");
  }
  return value;
}

function nonEmptyString(value: unknown, path: string): string {
  const result = stringValue(value, path);
  if (result.length === 0) {
    fail(path, "must be non-empty");
  }
  return result;
}

function boundedScalarString(
  value: unknown,
  path: string,
  minimum: number,
  maximum: number,
): string {
  const result = stringValue(value, path);
  const scalars = Array.from(result).length;
  if (scalars < minimum || scalars > maximum) {
    fail(path, `must contain ${minimum}..${maximum} Unicode scalars`);
  }
  return result;
}

function boundedUtf8String(
  value: unknown,
  path: string,
  minimum: number,
  maximum: number,
): string {
  const result = stringValue(value, path);
  const bytes = encoder.encode(result).byteLength;
  if (bytes < minimum || bytes > maximum) {
    fail(path, `must contain ${minimum}..${maximum} UTF-8 bytes`);
  }
  return result;
}

function booleanValue(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") {
    fail(path, "must be a boolean");
  }
  return value;
}

function nonNegativeSafeInteger(value: unknown, path: string): number {
  if (
    !Number.isSafeInteger(value) ||
    Number(value) < 0 ||
    Number(value) > MAX_SAFE_INTEGER
  ) {
    fail(path, "must be a non-negative safe integer");
  }
  return Number(value);
}

function positiveSafeInteger(value: unknown, path: string): number {
  if (!Number.isSafeInteger(value) || Number(value) <= 0) {
    fail(path, "must be a positive safe integer");
  }
  return Number(value);
}

function positiveIntegerAtMost(
  value: unknown,
  path: string,
  maximum: number,
): number {
  const result = positiveSafeInteger(value, path);
  if (result > maximum) {
    fail(path, `must be at most ${maximum}`);
  }
  return result;
}

function integerBetween(
  value: unknown,
  path: string,
  minimum: number,
  maximum: number,
): number {
  if (
    !Number.isSafeInteger(value) ||
    Number(value) < minimum ||
    Number(value) > maximum
  ) {
    fail(path, `must be an integer in ${minimum}..${maximum}`);
  }
  return Number(value);
}

function enumValue<const T>(
  value: unknown,
  path: string,
  allowed: readonly T[],
): T {
  if (!allowed.some((candidate) => Object.is(candidate, value))) {
    fail(path, `must be one of ${allowed.map(String).join(", ")}`);
  }
  return value as T;
}

function nullable<T>(
  value: unknown,
  path: string,
  read: (value: unknown, path: string) => T,
): T | null {
  return value === null ? null : read(value, path);
}

function uuid(value: unknown, path: string): string {
  const result = stringValue(value, path);
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(
      result,
    )
  ) {
    fail(path, "must be a canonical lowercase UUID");
  }
  return result;
}

function utcTimestamp(value: unknown, path: string): string {
  const result = stringValue(value, path);
  const match =
    /^(?<year>\d{4})-(?<month>\d{2})-(?<day>\d{2})T(?<hour>\d{2}):(?<minute>\d{2}):(?<second>\d{2})(?:\.\d{1,9})?Z$/.exec(
      result,
    );
  const groups = match?.groups;
  if (groups === undefined) {
    fail(path, "must be an RFC3339 UTC timestamp ending in Z");
  }
  const year = Number(groups.year);
  const month = Number(groups.month);
  const day = Number(groups.day);
  const hour = Number(groups.hour);
  const minute = Number(groups.minute);
  const second = Number(groups.second);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [
    31,
    leapYear ? 29 : 28,
    31,
    30,
    31,
    30,
    31,
    31,
    30,
    31,
    30,
    31,
  ][month - 1];
  if (
    month < 1 ||
    month > 12 ||
    daysInMonth === undefined ||
    day < 1 ||
    day > daysInMonth ||
    hour < 0 ||
    hour > 23 ||
    minute < 0 ||
    minute > 59 ||
    second < 0 ||
    second > 59
  ) {
    fail(path, "must be a valid RFC3339 UTC timestamp");
  }
  return result;
}

function lowerHex64(value: unknown, path: string): string {
  const result = stringValue(value, path);
  if (!/^[0-9a-f]{64}$/.test(result)) {
    fail(path, "must be exactly 64 lowercase hexadecimal characters");
  }
  return result;
}

function readReviewPath(value: unknown, path: string): string {
  const result = stringValue(value, path);
  const bytes = encoder.encode(result);
  const hasDrivePrefix = /^[A-Za-z]:/.test(result);
  if (
    bytes.length < 1 ||
    bytes.length > 4_096 ||
    result.startsWith("/") ||
    hasDrivePrefix ||
    result.includes("\\") ||
    result.includes("\0")
  ) {
    fail(path, "must be a canonical relative review path");
  }
  for (const component of result.split("/")) {
    const componentBytes = encoder.encode(component);
    const trimmed = component.replace(/[. ]+$/u, "");
    if (
      component.length === 0 ||
      component === "." ||
      component === ".." ||
      componentBytes.length > 255 ||
      component.includes(":") ||
      trimmed !== component ||
      component.toLowerCase() === ".git" ||
      isReservedDeviceName(component)
    ) {
      fail(path, "must be a canonical relative review path");
    }
  }
  return result;
}

function isReservedDeviceName(component: string): boolean {
  const stem = (component.split(".", 1)[0] ?? component).toUpperCase();
  return (
    ["CON", "PRN", "AUX", "NUL", "CLOCK$"].includes(stem) ||
    /^(?:COM|LPT)[1-9]$/.test(stem)
  );
}

function selectorKey(check: RequiredCheck): string {
  return check.kind === "cargo_check"
    ? `cargo_check\0${check.package ?? ""}`
    : `cargo_test\0${check.package ?? ""}\0${check.integration_test ?? ""}`;
}

function equalCheck(left: RequiredCheck, right: RequiredCheck): boolean {
  return (
    left.id === right.id &&
    left.kind === right.kind &&
    left.package === right.package &&
    (left.kind === "cargo_check" ||
      (right.kind === "cargo_test" &&
        left.integration_test === right.integration_test))
  );
}

function equalCheckArrays(
  left: readonly RequiredCheck[],
  right: readonly RequiredCheck[],
): boolean {
  return (
    left.length === right.length &&
    left.every((check, index) => {
      const other = right[index];
      return other !== undefined && equalCheck(check, other);
    })
  );
}

function startsWithChecks(
  full: readonly RequiredCheck[],
  prefix: readonly RequiredCheck[],
): boolean {
  return prefix.every((check, index) => {
    const candidate = full[index];
    return candidate !== undefined && equalCheck(candidate, check);
  });
}

function isOrderedCheckSubset(
  subset: readonly RequiredCheck[],
  full: readonly RequiredCheck[],
): boolean {
  let fullIndex = 0;
  for (const expected of subset) {
    let found = false;
    while (fullIndex < full.length) {
      const candidate = full[fullIndex];
      fullIndex += 1;
      if (candidate !== undefined && equalCheck(candidate, expected)) {
        found = true;
        break;
      }
    }
    if (!found) {
      return false;
    }
  }
  return true;
}

function equalDigest(
  left: ReviewEvidence["workspace_digest"],
  right: ReviewEvidence["workspace_digest"],
): boolean {
  return left.algorithm === right.algorithm && left.value === right.value;
}

function assertUniqueBy<T>(
  values: readonly T[],
  key: (value: T) => string,
  path: string,
  label: string,
): void {
  const seen = new Set<string>();
  for (const value of values) {
    const candidate = key(value);
    if (seen.has(candidate)) {
      fail(path, `duplicate ${label} ${JSON.stringify(candidate)}`);
    }
    seen.add(candidate);
  }
}

function encodedSizeAtMost(
  value: unknown,
  path: string,
  maximum: number,
  label: string,
): void {
  let encoded: Uint8Array;
  try {
    encoded = encoder.encode(JSON.stringify(value));
  } catch {
    fail(path, "must be JSON encodable");
  }
  if (encoded.byteLength > maximum) {
    fail(path, `canonical JSON must not exceed ${label}`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isUnicodeScalarString(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const low = value.charCodeAt(index + 1);
      if (!(low >= 0xdc00 && low <= 0xdfff)) {
        return false;
      }
      index += 1;
    } else if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) {
      return false;
    }
  }
  return true;
}

function fail(path: string, message: string): never {
  throw new ValidationError(path, message);
}
