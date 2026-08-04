import type {
  BootstrapResponse,
  Repository,
  SchedulerQueuedTask,
  SchedulerRepositoryStorage,
  SchedulerState,
  SchedulerStoppingTask,
  SchedulerStorage,
  SchedulerStorageScope,
  SchedulerStorageState,
  Task,
} from "./types";

const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;
const MAX_U32 = 0xffff_ffff;
const MAX_GLOBAL_TASKS = 4;
const MAX_QUEUED_TASKS = 256;
const MAX_CARGO_JOBS_PER_TASK = 8;

const STORAGE_STATES = [
  "normal",
  "pressure",
  "critical",
  "unavailable",
] as const;
const QUEUE_REASONS = [
  "service_paused",
  "storage_pressure",
  "global_capacity",
  "repository_capacity",
  "repository_control_busy",
] as const;
const STOP_INTENTS = ["user_cancelled", "disk_pressure_critical"] as const;

export class ValidationError extends Error {
  readonly path: string;

  constructor(path: string, message: string) {
    super(`${path}: ${message}`);
    this.name = "ValidationError";
    this.path = path;
  }
}

export interface SchedulerBootstrapValidationContext {
  readonly repositories: readonly Repository[];
  readonly tasks: readonly Task[];
  readonly latestEventId: number;
  readonly serverStartedAt: string;
  readonly serviceState: BootstrapResponse["service_state"];
  readonly serviceStateGeneration: number;
  readonly maxConcurrentTasks: number;
}

export interface SchedulerAuthorityValidationContext {
  readonly repositories: readonly Repository[];
  readonly tasks: readonly Task[];
  readonly serviceState: BootstrapResponse["service_state"];
}

export function validateSchedulerStateForBootstrap(
  value: unknown,
  context: SchedulerBootstrapValidationContext,
): SchedulerState {
  const scheduler = readSchedulerState(value);
  validateIntrinsicSchedulerProjection(scheduler);
  validateTopLevelAliases(scheduler, context);
  validateTaskProjection(scheduler, context.tasks);
  validateRepositoryStorage(scheduler.storage, context.repositories);
  return scheduler.value;
}

export function validateSchedulerState(value: unknown): SchedulerState {
  const scheduler = readSchedulerState(value);
  validateIntrinsicSchedulerProjection(scheduler);
  return scheduler.value;
}

export function validateSchedulerStateAgainstAuthority(
  value: unknown,
  context: SchedulerAuthorityValidationContext,
): SchedulerState {
  const scheduler = readSchedulerState(value);
  validateIntrinsicSchedulerProjection(scheduler);
  validateTaskProjection(scheduler, context.tasks);
  validateRepositoryStorage(scheduler.storage, context.repositories);
  validateAdmissionState(scheduler, context.serviceState);
  return scheduler.value;
}

interface ParsedSchedulerState
  extends ValidatedSchedulerAliases,
    ValidatedTaskProjection {
  readonly value: SchedulerState;
  readonly storage: SchedulerStorage;
}

function readSchedulerState(value: unknown): ParsedSchedulerState {
  const scheduler = exactObject(value, "$.scheduler", [
    "schema_version",
    "server_instance_id",
    "server_started_at",
    "generation",
    "as_of_event_id",
    "service_state_generation",
    "admission_state",
    "limits",
    "active_task_count",
    "queued_task_count",
    "queued_tasks",
    "stopping_tasks",
    "storage",
  ]);

  exactInteger(scheduler.schema_version, "$.scheduler.schema_version", 1);
  canonicalUuidV4(
    scheduler.server_instance_id,
    "$.scheduler.server_instance_id",
  );
  const serverStartedAt = utcTimestamp(
    scheduler.server_started_at,
    "$.scheduler.server_started_at",
  );
  nonNegativeSafeInteger(scheduler.generation, "$.scheduler.generation");
  const asOfEventId = nonNegativeSafeInteger(
    scheduler.as_of_event_id,
    "$.scheduler.as_of_event_id",
  );
  const serviceStateGeneration = nonNegativeSafeInteger(
    scheduler.service_state_generation,
    "$.scheduler.service_state_generation",
  );
  const admissionState = enumValue(
    scheduler.admission_state,
    "$.scheduler.admission_state",
    ["running", "paused"] as const,
  );
  const limits = readLimits(scheduler.limits);
  const activeTaskCount = u32(
    scheduler.active_task_count,
    "$.scheduler.active_task_count",
  );
  const queuedTaskCount = u32(
    scheduler.queued_task_count,
    "$.scheduler.queued_task_count",
  );
  const queuedTasks = readQueuedTasks(scheduler.queued_tasks);
  const stoppingTasks = readStoppingTasks(scheduler.stopping_tasks);
  const storage = readStorage(scheduler.storage);

  return {
    value: value as SchedulerState,
    serverStartedAt,
    serviceStateGeneration,
    asOfEventId,
    admissionState,
    globalLimit: limits.global,
    activeTaskCount,
    queuedTaskCount,
    queuedTasks,
    stoppingTasks,
    storage,
  };
}

function readLimits(value: unknown): SchedulerState["limits"] {
  const limits = exactObject(value, "$.scheduler.limits", [
    "global",
    "per_repository",
    "queued",
    "cargo_jobs_per_task",
  ]);
  const global = integerBetween(
    limits.global,
    "$.scheduler.limits.global",
    1,
    MAX_GLOBAL_TASKS,
  );
  const perRepository = integerBetween(
    limits.per_repository,
    "$.scheduler.limits.per_repository",
    1,
    MAX_GLOBAL_TASKS,
  );
  if (perRepository > global) {
    fail(
      "$.scheduler.limits.per_repository",
      "must not exceed limits.global",
    );
  }
  integerBetween(
    limits.queued,
    "$.scheduler.limits.queued",
    1,
    MAX_QUEUED_TASKS,
  );
  integerBetween(
    limits.cargo_jobs_per_task,
    "$.scheduler.limits.cargo_jobs_per_task",
    1,
    MAX_CARGO_JOBS_PER_TASK,
  );
  return value as SchedulerState["limits"];
}

function readQueuedTasks(value: unknown): SchedulerQueuedTask[] {
  return readArray(value, "$.scheduler.queued_tasks", (item, path) => {
    const queued = exactObject(item, path, ["task_id", "reason"]);
    canonicalUuid(queued.task_id, `${path}.task_id`);
    enumValue(queued.reason, `${path}.reason`, QUEUE_REASONS);
    return item as SchedulerQueuedTask;
  });
}

function readStoppingTasks(value: unknown): SchedulerStoppingTask[] {
  return readArray(value, "$.scheduler.stopping_tasks", (item, path) => {
    const stopping = exactObject(item, path, ["task_id", "intent"]);
    canonicalUuid(stopping.task_id, `${path}.task_id`);
    enumValue(stopping.intent, `${path}.intent`, STOP_INTENTS);
    return item as SchedulerStoppingTask;
  });
}

function readStorage(value: unknown): SchedulerStorage {
  const storage = exactObject(value, "$.scheduler.storage", [
    "state",
    "data",
    "runtime",
    "repositories",
  ]);
  const state = storageState(storage.state, "$.scheduler.storage.state");
  const data = readStorageScope(storage.data, "$.scheduler.storage.data");
  const runtime = readStorageScope(
    storage.runtime,
    "$.scheduler.storage.runtime",
  );
  const repositories = readArray(
    storage.repositories,
    "$.scheduler.storage.repositories",
    readRepositoryStorage,
  );
  const aggregate = aggregateStorageState([
    data.state,
    runtime.state,
    ...repositories.map((repository) => repository.state),
  ]);
  if (state !== aggregate) {
    fail(
      "$.scheduler.storage.state",
      `must equal the logical scope aggregate ${aggregate}`,
    );
  }
  return value as SchedulerStorage;
}

function readStorageScope(
  value: unknown,
  path: string,
): SchedulerStorageScope {
  const scope = exactObject(value, path, ["state"]);
  storageState(scope.state, `${path}.state`);
  return value as SchedulerStorageScope;
}

function readRepositoryStorage(
  value: unknown,
  path: string,
): SchedulerRepositoryStorage {
  const repository = exactObject(value, path, ["repository_id", "state"]);
  canonicalUuid(repository.repository_id, `${path}.repository_id`);
  storageState(repository.state, `${path}.state`);
  return value as SchedulerRepositoryStorage;
}

export function aggregateStorageState(
  states: readonly SchedulerStorageState[],
): SchedulerStorageState {
  if (states.includes("critical")) {
    return "critical";
  }
  if (states.includes("unavailable")) {
    return "unavailable";
  }
  if (states.includes("pressure")) {
    return "pressure";
  }
  return "normal";
}

interface ValidatedSchedulerAliases {
  readonly serverStartedAt: string;
  readonly serviceStateGeneration: number;
  readonly asOfEventId: number;
  readonly admissionState: SchedulerState["admission_state"];
  readonly globalLimit: number;
}

function validateTopLevelAliases(
  scheduler: ValidatedSchedulerAliases,
  context: SchedulerBootstrapValidationContext,
): void {
  if (scheduler.globalLimit !== context.maxConcurrentTasks) {
    fail(
      "$.scheduler.limits.global",
      "must equal BootstrapResponse.max_concurrent_tasks",
    );
  }
  if (scheduler.serverStartedAt !== context.serverStartedAt) {
    fail(
      "$.scheduler.server_started_at",
      "must equal BootstrapResponse.server_started_at",
    );
  }
  if (scheduler.serviceStateGeneration !== context.serviceStateGeneration) {
    fail(
      "$.scheduler.service_state_generation",
      "must equal BootstrapResponse.service_state_generation",
    );
  }
  if (scheduler.asOfEventId > context.latestEventId) {
    fail(
      "$.scheduler.as_of_event_id",
      "must not exceed BootstrapResponse.latest_event_id",
    );
  }
  validateAdmissionState(scheduler, context.serviceState);
}

function validateAdmissionState(
  scheduler: Pick<ParsedSchedulerState, "admissionState">,
  serviceState: BootstrapResponse["service_state"],
): void {
  if (serviceState !== "ready" && scheduler.admissionState !== "paused") {
    fail(
      "$.scheduler.admission_state",
      "must be paused while service_state is not ready",
    );
  }
}

interface ValidatedTaskProjection {
  readonly activeTaskCount: number;
  readonly queuedTaskCount: number;
  readonly queuedTasks: readonly SchedulerQueuedTask[];
  readonly stoppingTasks: readonly SchedulerStoppingTask[];
  readonly globalLimit: number;
}

function validateIntrinsicSchedulerProjection(
  scheduler: ParsedSchedulerState,
): void {
  if (scheduler.activeTaskCount > scheduler.globalLimit) {
    fail(
      "$.scheduler.active_task_count",
      "must not exceed limits.global",
    );
  }
  if (scheduler.queuedTaskCount !== scheduler.queuedTasks.length) {
    fail(
      "$.scheduler.queued_task_count",
      "must equal queued_tasks.length",
    );
  }
  if (scheduler.stoppingTasks.length > scheduler.activeTaskCount) {
    fail(
      "$.scheduler.stopping_tasks",
      "length must not exceed active_task_count",
    );
  }

  const taskIds = new Set<string>();
  for (const [index, queued] of scheduler.queuedTasks.entries()) {
    if (taskIds.has(queued.task_id)) {
      fail(
        `$.scheduler.queued_tasks[${index}].task_id`,
        "must be unique across queued and stopping tasks",
      );
    }
    taskIds.add(queued.task_id);
  }
  for (const [index, stopping] of scheduler.stoppingTasks.entries()) {
    if (taskIds.has(stopping.task_id)) {
      fail(
        `$.scheduler.stopping_tasks[${index}].task_id`,
        "must be unique across queued and stopping tasks",
      );
    }
    taskIds.add(stopping.task_id);
  }

  let previousRepositoryId: string | undefined;
  for (const [index, repository] of scheduler.storage.repositories.entries()) {
    if (
      previousRepositoryId !== undefined &&
      previousRepositoryId >= repository.repository_id
    ) {
      fail(
        `$.scheduler.storage.repositories[${index}].repository_id`,
        "must be unique and in canonical UUID order",
      );
    }
    previousRepositoryId = repository.repository_id;
  }
}

function validateTaskProjection(
  scheduler: ValidatedTaskProjection,
  tasks: readonly Task[],
): void {
  const runningTaskIds = new Set(
    tasks.filter((task) => task.status === "running").map((task) => task.id),
  );
  if (scheduler.activeTaskCount < runningTaskIds.size) {
    fail(
      "$.scheduler.active_task_count",
      "must not be below the number of current Running tasks",
    );
  }
  if (scheduler.activeTaskCount > scheduler.globalLimit) {
    fail(
      "$.scheduler.active_task_count",
      "must not exceed limits.global",
    );
  }
  if (scheduler.stoppingTasks.length > scheduler.activeTaskCount) {
    fail(
      "$.scheduler.stopping_tasks",
      "length must not exceed active_task_count",
    );
  }
  validateQueuedProjection(
    scheduler.queuedTasks,
    scheduler.queuedTaskCount,
    tasks,
  );
  validateStoppingProjection(scheduler.stoppingTasks, runningTaskIds);
}

function validateQueuedProjection(
  queuedTasks: readonly SchedulerQueuedTask[],
  queuedTaskCount: number,
  tasks: readonly Task[],
): void {
  if (queuedTaskCount !== queuedTasks.length) {
    fail(
      "$.scheduler.queued_task_count",
      "must equal queued_tasks.length",
    );
  }
  const expected = tasks
    .filter((task) => task.status === "queued")
    .sort(compareQueuedTasks);
  if (queuedTasks.length !== expected.length) {
    fail(
      "$.scheduler.queued_tasks",
      "must exactly cover current Queued tasks",
    );
  }
  for (const [index, task] of expected.entries()) {
    if (queuedTasks[index]?.task_id !== task.id) {
      fail(
        `$.scheduler.queued_tasks[${index}].task_id`,
        "must follow authoritative (created_at, task_id) order and coverage",
      );
    }
  }
}

function validateStoppingProjection(
  stoppingTasks: readonly SchedulerStoppingTask[],
  runningTaskIds: ReadonlySet<string>,
): void {
  // The wire deliberately omits requested_at and the durable intent table.
  // Validate every client-observable invariant without guessing either the
  // server's first-winner classification or its hidden primary sort key.
  const seen = new Set<string>();
  for (const [index, stopping] of stoppingTasks.entries()) {
    const path = `$.scheduler.stopping_tasks[${index}].task_id`;
    if (seen.has(stopping.task_id)) {
      fail(path, "must be unique");
    }
    seen.add(stopping.task_id);
    if (!runningTaskIds.has(stopping.task_id)) {
      fail(path, "must reference a current Running task");
    }
  }
}

function validateRepositoryStorage(
  storage: SchedulerStorage,
  repositories: readonly Repository[],
): void {
  const expectedIds = repositories
    .map((repository) => repository.id)
    .sort(compareStrings);
  const actualIds = storage.repositories.map(
    (repository) => repository.repository_id,
  );
  if (actualIds.length !== expectedIds.length) {
    fail(
      "$.scheduler.storage.repositories",
      "must exactly cover Bootstrap repositories",
    );
  }
  for (const [index, expectedId] of expectedIds.entries()) {
    if (actualIds[index] !== expectedId) {
      fail(
        `$.scheduler.storage.repositories[${index}].repository_id`,
        "must exactly cover Bootstrap repositories in canonical UUID order",
      );
    }
  }
}

function compareQueuedTasks(left: Task, right: Task): number {
  const byCreatedAt = compareStrings(
    normalizeUtcSortKey(left.created_at),
    normalizeUtcSortKey(right.created_at),
  );
  return byCreatedAt === 0 ? compareStrings(left.id, right.id) : byCreatedAt;
}

function normalizeUtcSortKey(value: string): string {
  const match = /^(?<seconds>.{19})(?:\.(?<fraction>\d{1,9}))?Z$/.exec(value);
  const groups = match?.groups;
  if (groups?.seconds === undefined) {
    return value;
  }
  return `${groups.seconds}.${(groups.fraction ?? "").padEnd(9, "0")}`;
}

function compareStrings(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
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
  read: (item: unknown, itemPath: string) => T,
): T[] {
  if (!Array.isArray(value)) {
    fail(path, "must be an array");
  }
  return value.map((item, index) => read(item, `${path}[${index}]`));
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

function u32(value: unknown, path: string): number {
  return integerBetween(value, path, 0, MAX_U32);
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

function exactInteger(
  value: unknown,
  path: string,
  expected: number,
): number {
  const result = integerBetween(value, path, expected, expected);
  return result;
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

function storageState(
  value: unknown,
  path: string,
): SchedulerStorageState {
  return enumValue(value, path, STORAGE_STATES);
}

function canonicalUuid(value: unknown, path: string): string {
  if (
    typeof value !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(
      value,
    )
  ) {
    fail(path, "must be a canonical lowercase UUID");
  }
  return value;
}

function canonicalUuidV4(value: unknown, path: string): string {
  const result = canonicalUuid(value, path);
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
      result,
    )
  ) {
    fail(path, "must be a canonical lowercase UUID v4");
  }
  return result;
}

function utcTimestamp(value: unknown, path: string): string {
  if (!isValidRfc3339UtcTimestamp(value)) {
    fail(path, "must be an RFC3339 UTC timestamp ending in Z");
  }
  return value;
}

export function isValidRfc3339UtcTimestamp(
  value: unknown,
): value is string {
  if (typeof value !== "string") {
    return false;
  }
  const match =
    /^(?<year>\d{4})-(?<month>\d{2})-(?<day>\d{2})T(?<hour>\d{2}):(?<minute>\d{2}):(?<second>\d{2})(?:\.\d{1,9})?Z$/.exec(
      value,
    );
  const groups = match?.groups;
  if (groups === undefined) {
    return false;
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
  return (
    month >= 1 &&
    month <= 12 &&
    daysInMonth !== undefined &&
    day >= 1 &&
    day <= daysInMonth &&
    hour <= 23 &&
    minute <= 59 &&
    second <= 59
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function fail(path: string, message: string): never {
  throw new ValidationError(path, message);
}
