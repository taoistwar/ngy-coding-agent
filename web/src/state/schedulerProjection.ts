import type { SchedulerState } from "../api/types";
import {
  canonicalizeSchedulerState,
  type SchedulerSnapshotCandidate,
} from "../api/schedulerSnapshot";

export type SchedulerFreshness = "unavailable" | "fresh" | "stale";

export type SchedulerRecoveryReason =
  | "scheduler_instance_changed"
  | "scheduler_started_at_changed"
  | "scheduler_generation_conflict"
  | "scheduler_causal_tuple_incomparable"
  | "scheduler_event_watermark_impossible"
  | "scheduler_authority_mismatch";

export interface SchedulerProjectionCandidate {
  readonly snapshot: SchedulerState;
  readonly digest: string | null;
  readonly canonicalJson: string;
}

export interface SchedulerProjectionState {
  readonly snapshot: SchedulerState | null;
  readonly freshness: SchedulerFreshness;
  readonly staleReason: string | null;
  readonly digest: string | null;
  readonly canonicalJson: string | null;
  readonly pending: SchedulerProjectionCandidate | null;
  readonly recoveryReason: SchedulerRecoveryReason | null;
}

export const initialSchedulerProjection: SchedulerProjectionState = {
  snapshot: null,
  freshness: "unavailable",
  staleReason: null,
  digest: null,
  canonicalJson: null,
  pending: null,
  recoveryReason: null,
};

export function acceptSchedulerCandidate(
  projection: SchedulerProjectionState,
  candidate: SchedulerSnapshotCandidate,
  appliedTaskEventId: number,
  appliedMembershipEventId: number,
  serviceGeneration: number,
): SchedulerProjectionState {
  if (projection.recoveryReason !== null) {
    return projection;
  }
  return arbitrateCandidate(
    projection,
    {
      snapshot: candidate.snapshot,
      digest: candidate.digest,
      canonicalJson: candidate.canonicalJson,
    },
    appliedTaskEventId,
    appliedMembershipEventId,
    serviceGeneration,
  );
}

export function adoptSchedulerBootstrap(
  projection: SchedulerProjectionState,
  snapshot: SchedulerState,
  appliedTaskEventId: number,
  appliedMembershipEventId: number,
  serviceGeneration: number,
): SchedulerProjectionState {
  const recoveredProjection =
    projection.recoveryReason === null
      ? projection
      : initialSchedulerProjection;
  return arbitrateCandidate(
    recoveredProjection,
    {
      snapshot,
      digest: null,
      canonicalJson: canonicalizeSchedulerState(snapshot),
    },
    appliedTaskEventId,
    appliedMembershipEventId,
    serviceGeneration,
  );
}

export function advanceSchedulerCausalPosition(
  projection: SchedulerProjectionState,
  appliedTaskEventId: number,
  appliedMembershipEventId: number,
  serviceGeneration: number,
): SchedulerProjectionState {
  if (projection.recoveryReason !== null) {
    return projection;
  }
  if (projection.pending !== null) {
    return gateCandidate(
      projection,
      projection.pending,
      appliedTaskEventId,
      appliedMembershipEventId,
      serviceGeneration,
    );
  }
  return reconcileCurrentCausalPosition(
    projection,
    appliedTaskEventId,
    appliedMembershipEventId,
    serviceGeneration,
  );
}

export function markSchedulerStale(
  projection: SchedulerProjectionState,
  reason: string,
): SchedulerProjectionState {
  if (projection.snapshot === null || projection.freshness === "stale") {
    return projection;
  }
  return {
    ...projection,
    freshness: "stale",
    staleReason: reason,
  };
}

function arbitrateCandidate(
  projection: SchedulerProjectionState,
  candidate: SchedulerProjectionCandidate,
  appliedTaskEventId: number,
  appliedMembershipEventId: number,
  serviceGeneration: number,
): SchedulerProjectionState {
  const baseline = newestCandidate(projection);
  if (baseline !== null) {
    if (
      candidate.snapshot.server_instance_id !==
      baseline.snapshot.server_instance_id
    ) {
      return clearSchedulerForRecovery("scheduler_instance_changed");
    }
    if (
      candidate.snapshot.server_started_at !==
      baseline.snapshot.server_started_at
    ) {
      return clearSchedulerForRecovery("scheduler_started_at_changed");
    }
    if (candidate.snapshot.generation < baseline.snapshot.generation) {
      return advanceSchedulerCausalPosition(
        projection,
        appliedTaskEventId,
        appliedMembershipEventId,
        serviceGeneration,
      );
    }
    if (candidate.snapshot.generation === baseline.snapshot.generation) {
      if (!samePayload(candidate, baseline)) {
        return clearSchedulerForRecovery("scheduler_generation_conflict");
      }
      candidate = mergeEquivalentCandidate(baseline, candidate);
    }
  }
  return gateCandidate(
    projection,
    candidate,
    appliedTaskEventId,
    appliedMembershipEventId,
    serviceGeneration,
  );
}

function gateCandidate(
  projection: SchedulerProjectionState,
  candidate: SchedulerProjectionCandidate,
  appliedTaskEventId: number,
  appliedMembershipEventId: number,
  serviceGeneration: number,
): SchedulerProjectionState {
  if (
    appliedMembershipEventId < candidate.snapshot.as_of_event_id &&
    appliedTaskEventId >= candidate.snapshot.as_of_event_id
  ) {
    return clearSchedulerForRecovery("scheduler_event_watermark_impossible");
  }
  const membership = compare(
    appliedMembershipEventId,
    candidate.snapshot.as_of_event_id,
  );
  const service = compare(
    serviceGeneration,
    candidate.snapshot.service_state_generation,
  );

  if (membership === 0 && service === 0) {
    return installCandidate(candidate);
  }
  if (membership <= 0 && service <= 0) {
    return reconcileCurrentCausalPosition(
      { ...projection, pending: candidate },
      appliedTaskEventId,
      appliedMembershipEventId,
      serviceGeneration,
    );
  }
  if (membership >= 0 && service >= 0) {
    return reconcileCurrentCausalPosition(
      { ...projection, pending: null },
      appliedTaskEventId,
      appliedMembershipEventId,
      serviceGeneration,
    );
  }
  return clearSchedulerForRecovery("scheduler_causal_tuple_incomparable");
}

function reconcileCurrentCausalPosition(
  projection: SchedulerProjectionState,
  appliedTaskEventId: number,
  appliedMembershipEventId: number,
  serviceGeneration: number,
): SchedulerProjectionState {
  const current = currentCandidate(projection);
  if (current === null) {
    return projection;
  }
  if (
    appliedMembershipEventId < current.snapshot.as_of_event_id &&
    appliedTaskEventId >= current.snapshot.as_of_event_id
  ) {
    return clearSchedulerForRecovery("scheduler_event_watermark_impossible");
  }
  const membership = compare(
    appliedMembershipEventId,
    current.snapshot.as_of_event_id,
  );
  const service = compare(
    serviceGeneration,
    current.snapshot.service_state_generation,
  );

  if (membership === 0 && service === 0) {
    return projection;
  }
  if (membership <= 0 && service <= 0) {
    return markSchedulerStale(
      { ...projection, pending: projection.pending ?? current },
      "bootstrap_causal_position_behind",
    );
  }
  if (membership >= 0 && service >= 0) {
    const reason =
      membership > 0
        ? "membership_event_advanced"
        : "service_generation_advanced";
    return markSchedulerStale(projection, reason);
  }
  return clearSchedulerForRecovery("scheduler_causal_tuple_incomparable");
}

function newestCandidate(
  projection: SchedulerProjectionState,
): SchedulerProjectionCandidate | null {
  const current = currentCandidate(projection);
  if (current === null) {
    return projection.pending;
  }
  if (
    projection.pending !== null &&
    projection.pending.snapshot.generation > current.snapshot.generation
  ) {
    return projection.pending;
  }
  return current;
}

function currentCandidate(
  projection: SchedulerProjectionState,
): SchedulerProjectionCandidate | null {
  if (
    projection.snapshot === null ||
    projection.canonicalJson === null
  ) {
    return null;
  }
  return {
    snapshot: projection.snapshot,
    digest: projection.digest,
    canonicalJson: projection.canonicalJson,
  };
}

function samePayload(
  left: SchedulerProjectionCandidate,
  right: SchedulerProjectionCandidate,
): boolean {
  if (left.canonicalJson !== right.canonicalJson) {
    return false;
  }
  return (
    left.digest === null ||
    right.digest === null ||
    left.digest === right.digest
  );
}

function mergeEquivalentCandidate(
  baseline: SchedulerProjectionCandidate,
  incoming: SchedulerProjectionCandidate,
): SchedulerProjectionCandidate {
  return {
    snapshot: incoming.snapshot,
    canonicalJson: incoming.canonicalJson,
    digest: incoming.digest ?? baseline.digest,
  };
}

function installCandidate(
  candidate: SchedulerProjectionCandidate,
): SchedulerProjectionState {
  return {
    snapshot: candidate.snapshot,
    freshness: "fresh",
    staleReason: null,
    digest: candidate.digest,
    canonicalJson: candidate.canonicalJson,
    pending: null,
    recoveryReason: null,
  };
}

export function clearSchedulerForRecovery(
  reason: SchedulerRecoveryReason,
): SchedulerProjectionState {
  return {
    ...initialSchedulerProjection,
    recoveryReason: reason,
  };
}

function compare(left: number, right: number): -1 | 0 | 1 {
  return left < right ? -1 : left > right ? 1 : 0;
}
