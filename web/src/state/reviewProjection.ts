import type {
  ReviewEvidence,
  TaskDetail,
  TaskEvent,
} from "../api/types";
import {
  ValidationError,
  validateTaskDetail,
} from "../api/validation";

export type ReviewUpdatedEvent = Extract<
  TaskEvent,
  { kind: "review.updated" }
>;

export type ReviewProjectionResult =
  | { kind: "applied"; detail: TaskDetail }
  | { kind: "replayed"; detail: TaskDetail }
  | {
      kind: "stale";
      detail: TaskDetail;
      reason: "review_history_incomplete";
    }
  | {
      kind: "conflict";
      reason:
        | "review_payload_conflict"
        | "review_history_conflict"
        | "review_task_conflict";
    };

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (typeof value !== "object" || value === null) {
    return value;
  }

  const canonical: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    const field = (value as Record<string, unknown>)[key];
    if (field !== undefined) {
      canonical[key] = canonicalize(field);
    }
  }
  return canonical;
}

export function canonicalReviewPayload(review: ReviewEvidence): string {
  return JSON.stringify(canonicalize(review));
}

function advanceDetailCursor(
  detail: TaskDetail,
  event: ReviewUpdatedEvent,
): TaskDetail {
  if (event.id <= detail.event_cursor) {
    return detail;
  }
  return {
    ...detail,
    task:
      detail.task.last_event_id >= event.id
        ? detail.task
        : { ...detail.task, last_event_id: event.id },
    event_cursor: event.id,
  };
}

/**
 * Projects one validated, self-contained review event against the complete
 * history held by a TaskDetail snapshot. Cross-round validation deliberately
 * delegates to the shared REST validator so reducer and HTTP ingestion cannot
 * drift.
 */
export function projectReviewEvent(
  detail: TaskDetail,
  event: ReviewUpdatedEvent,
): ReviewProjectionResult {
  if (event.task_id !== detail.task.id) {
    return { kind: "conflict", reason: "review_task_conflict" };
  }

  const review = event.payload.review;
  const existing = detail.reviews.find(
    (candidate) => candidate.round === review.round,
  );
  if (existing !== undefined) {
    if (canonicalReviewPayload(existing) !== canonicalReviewPayload(review)) {
      return { kind: "conflict", reason: "review_payload_conflict" };
    }
    return { kind: "replayed", detail: advanceDetailCursor(detail, event) };
  }

  const expectedRound = detail.reviews.length + 1;
  if (detail.plan == null || review.round > expectedRound) {
    return {
      kind: "stale",
      detail: advanceDetailCursor(detail, event),
      reason: "review_history_incomplete",
    };
  }
  if (review.round < expectedRound) {
    return { kind: "conflict", reason: "review_history_conflict" };
  }

  const advanced = advanceDetailCursor(detail, event);
  const candidate: TaskDetail = {
    ...advanced,
    reviews: [...advanced.reviews, review],
  };
  try {
    return { kind: "applied", detail: validateTaskDetail(candidate) };
  } catch (error) {
    if (error instanceof ValidationError) {
      return { kind: "conflict", reason: "review_history_conflict" };
    }
    throw error;
  }
}
