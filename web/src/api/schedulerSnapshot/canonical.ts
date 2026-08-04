import type { SchedulerState } from "../types";
import { validateSchedulerState } from "../schedulerValidation";
import { fail } from "./error";

export function canonicalizeSchedulerState(snapshot: SchedulerState): string {
  const validated = validateSchedulerState(snapshot);
  return canonicalizeRestricted(validated);
}

export function canonicalizeSchedulerString(value: string): string {
  return canonicalizeRestricted(value);
}

export async function schedulerStateDigest(
  snapshot: SchedulerState,
): Promise<string> {
  const bytes = new TextEncoder().encode(canonicalizeSchedulerState(snapshot));
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

export function canonicalizeRestricted(value: unknown): string {
  if (typeof value === "string") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || value < 0) {
      fail("$.scheduler", "canonical values must be non-negative safe integers");
    }
    return String(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalizeRestricted).join(",")}]`;
  }
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .sort()
      .map(
        (key) =>
          `${JSON.stringify(key)}:${canonicalizeRestricted(value[key])}`,
      )
      .join(",")}}`;
  }
  fail("$.scheduler", "canonical values must stay inside the Scheduler DTO domain");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
