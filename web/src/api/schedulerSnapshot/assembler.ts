import type {
  SchedulerState,
  SchedulerStateChunkControl,
  SchedulerStateControl,
  SchedulerStateItem,
} from "../types";
import {
  ValidationError,
  validateSchedulerState,
} from "../schedulerValidation";
import {
  canonicalizeRestricted,
  canonicalizeSchedulerState,
  schedulerStateDigest,
} from "./canonical";
import { SchedulerSnapshotError, fail } from "./error";
import {
  validateSchedulerStateChunkControl,
  validateSchedulerStateControl,
} from "./wireValidation";

export interface SchedulerSnapshotCandidate {
  readonly snapshot: SchedulerState;
  readonly digest: string;
  readonly canonicalJson: string;
}

export type SchedulerAssemblerOutcome =
  | { readonly kind: "pending" }
  | { readonly kind: "complete"; readonly candidate: SchedulerSnapshotCandidate }
  | { readonly kind: "ignored"; readonly reason: "stale_generation" };

interface PartialSnapshot {
  readonly manifest: SchedulerStateControl;
  readonly manifestCanonical: string;
  readonly items: SchedulerStateItem[];
  nextChunkIndex: number;
}

interface AcceptedGeneration {
  readonly serverInstanceId: string;
  readonly generation: number;
  readonly manifestCanonical: string;
}

export class SchedulerSnapshotAssembler {
  #partial: PartialSnapshot | null = null;
  #accepted: AcceptedGeneration | null = null;

  get hasPartial(): boolean {
    return this.#partial !== null;
  }

  reset(): void {
    this.#partial = null;
    this.#accepted = null;
  }

  async accept(control: unknown): Promise<SchedulerAssemblerOutcome> {
    if (!isRecord(control) || typeof control.kind !== "string") {
      fail("$.scheduler_control", "must be a scheduler control object");
    }
    if (control.kind === "scheduler.state") {
      return this.#acceptManifest(validateSchedulerStateControl(control));
    }
    if (control.kind === "scheduler.state.chunk") {
      return this.#acceptChunk(validateSchedulerStateChunkControl(control));
    }
    fail("$.scheduler_control.kind", "is not a scheduler control kind");
  }

  async #acceptManifest(
    manifest: SchedulerStateControl,
  ): Promise<SchedulerAssemblerOutcome> {
    const manifestCanonical = canonicalizeRestricted(manifest);
    const current = this.#partial;
    const baseline =
      current === null
        ? this.#accepted
        : generationOf(current.manifest, current.manifestCanonical);
    if (baseline !== null) {
      if (manifest.server_instance_id !== baseline.serverInstanceId) {
        this.#partial = null;
        fail(
          "$.scheduler_control.server_instance_id",
          "changed while scheduler state was comparable; bootstrap recovery is required",
        );
      }
      if (manifest.generation < baseline.generation) {
        return { kind: "ignored", reason: "stale_generation" };
      }
      if (manifest.generation === baseline.generation) {
        if (current !== null) {
          this.#partial = null;
          fail(
            "$.scheduler_control.generation",
            "duplicate manifest interrupted an in-flight generation",
          );
        }
        if (baseline.manifestCanonical !== manifestCanonical) {
          this.#partial = null;
          fail(
            "$.scheduler_control.generation",
            "same generation carried a different manifest or digest",
          );
        }
      }
    }

    if (manifest.chunk_count === 0) {
      this.#partial = null;
      const candidate = await completeSnapshot(manifest, []);
      this.#accepted = generationOf(manifest, manifestCanonical);
      return { kind: "complete", candidate };
    }
    this.#partial = {
      manifest,
      manifestCanonical,
      items: [],
      nextChunkIndex: 0,
    };
    return { kind: "pending" };
  }

  async #acceptChunk(
    chunk: SchedulerStateChunkControl,
  ): Promise<SchedulerAssemblerOutcome> {
    const partial = this.#partial;
    if (partial === null) {
      const accepted = this.#accepted;
      if (accepted !== null) {
        if (chunk.server_instance_id !== accepted.serverInstanceId) {
          fail(
            "$.scheduler_chunk.server_instance_id",
            "changed while scheduler state was comparable; bootstrap recovery is required",
          );
        }
        if (chunk.generation < accepted.generation) {
          return { kind: "ignored", reason: "stale_generation" };
        }
        if (chunk.generation === accepted.generation) {
          fail(
            "$.scheduler_chunk.chunk_index",
            "duplicate chunk after completion",
          );
        }
      }
      fail("$.scheduler_chunk", "arrived without a matching manifest");
    }
    const manifest = partial.manifest;
    if (chunk.server_instance_id !== manifest.server_instance_id) {
      this.#partial = null;
      fail("$.scheduler_chunk.server_instance_id", "does not match the manifest");
    }
    if (chunk.generation < manifest.generation) {
      return { kind: "ignored", reason: "stale_generation" };
    }
    if (chunk.generation !== manifest.generation) {
      this.#partial = null;
      fail("$.scheduler_chunk.generation", "does not match the manifest");
    }
    if (chunk.snapshot_digest !== manifest.snapshot_digest) {
      this.#partial = null;
      fail("$.scheduler_chunk.snapshot_digest", "does not match the manifest");
    }
    if (chunk.chunk_count !== manifest.chunk_count) {
      this.#partial = null;
      fail("$.scheduler_chunk.chunk_count", "does not match the manifest");
    }
    if (chunk.chunk_index < partial.nextChunkIndex) {
      this.#partial = null;
      fail("$.scheduler_chunk.chunk_index", "duplicate chunk");
    }
    if (chunk.chunk_index > partial.nextChunkIndex) {
      this.#partial = null;
      fail("$.scheduler_chunk.chunk_index", "out-of-order or missing chunk");
    }
    partial.items.push(...chunk.items);
    partial.nextChunkIndex += 1;
    if (partial.nextChunkIndex < manifest.chunk_count) {
      return { kind: "pending" };
    }
    if (partial.items.length !== manifest.item_count) {
      this.#partial = null;
      fail(
        "$.scheduler_chunk.items",
        "assembled item count does not match manifest",
      );
    }
    this.#partial = null;
    const candidate = await completeSnapshot(manifest, [...partial.items]);
    this.#accepted = generationOf(manifest, partial.manifestCanonical);
    return { kind: "complete", candidate };
  }
}

async function completeSnapshot(
  manifest: SchedulerStateControl,
  items: readonly SchedulerStateItem[],
): Promise<SchedulerSnapshotCandidate> {
  const queuedEnd = manifest.queued_task_count;
  const stoppingEnd = queuedEnd + manifest.stopping_task_count;
  const repositoryEnd = stoppingEnd + manifest.repository_storage_count;
  if (items.length !== repositoryEnd) {
    fail("$.scheduler_chunk.items", "does not match the manifest group counts");
  }
  const queued = items.slice(0, queuedEnd);
  const stopping = items.slice(queuedEnd, stoppingEnd);
  const repositories = items.slice(stoppingEnd, repositoryEnd);
  if (queued.some((item) => item.kind !== "queued_task")) {
    fail(
      "$.scheduler_chunk.items",
      "violates canonical item order for queued tasks",
    );
  }
  if (stopping.some((item) => item.kind !== "stopping_task")) {
    fail(
      "$.scheduler_chunk.items",
      "violates canonical item order for stopping tasks",
    );
  }
  if (repositories.some((item) => item.kind !== "repository_storage")) {
    fail(
      "$.scheduler_chunk.items",
      "violates canonical item order for repository storage",
    );
  }
  const snapshot: SchedulerState = {
    schema_version: 1,
    server_instance_id: manifest.server_instance_id,
    server_started_at: manifest.server_started_at,
    generation: manifest.generation,
    as_of_event_id: manifest.as_of_event_id,
    service_state_generation: manifest.service_state_generation,
    admission_state: manifest.admission_state,
    limits: manifest.limits,
    active_task_count: manifest.active_task_count,
    queued_task_count: manifest.queued_task_count,
    queued_tasks: queued.map((item) => {
      if (item.kind !== "queued_task") {
        throw new Error("validated queued item changed kind");
      }
      return { task_id: item.task_id, reason: item.reason };
    }),
    stopping_tasks: stopping.map((item) => {
      if (item.kind !== "stopping_task") {
        throw new Error("validated stopping item changed kind");
      }
      return { task_id: item.task_id, intent: item.intent };
    }),
    storage: {
      state: manifest.storage.state,
      data: manifest.storage.data,
      runtime: manifest.storage.runtime,
      repositories: repositories.map((item) => {
        if (item.kind !== "repository_storage") {
          throw new Error("validated repository item changed kind");
        }
        return { repository_id: item.repository_id, state: item.state };
      }),
    },
  };
  try {
    validateSchedulerState(snapshot);
  } catch (error) {
    if (error instanceof ValidationError) {
      throw new SchedulerSnapshotError(error.path, error.message, {
        cause: error,
      });
    }
    throw error;
  }
  const canonicalJson = canonicalizeSchedulerState(snapshot);
  const digest = await schedulerStateDigest(snapshot);
  if (digest !== manifest.snapshot_digest) {
    fail(
      "$.scheduler_control.snapshot_digest",
      "does not match assembled snapshot",
    );
  }
  return { snapshot, digest, canonicalJson };
}

function generationOf(
  value: SchedulerStateControl,
  manifestCanonical: string,
): AcceptedGeneration {
  return {
    serverInstanceId: value.server_instance_id,
    generation: value.generation,
    manifestCanonical,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
