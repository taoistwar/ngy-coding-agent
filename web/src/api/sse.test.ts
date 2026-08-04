import { describe, expect, it, vi } from "vitest";

import type {
  ReviewEvidence,
  SchedulerState,
  SchedulerStateChunkControl,
  SchedulerStateControl,
  SseMessage,
} from "./types";
import {
  schedulerStateDigest,
  type SchedulerSnapshotCandidate,
} from "./schedulerSnapshot";
import {
  IncrementalSseParser,
  SseClient,
  SseProtocolError,
  computeBackoffDelay,
  type SseClientCallbacks,
  type SseClientState,
  type SseRecoveryReason,
} from "./sse";

const encoder = new TextEncoder();

function taskQueued(id: number): Extract<SseMessage, { kind: "task.queued" }> {
  return {
    id,
    schema_version: 1,
    kind: "task.queued",
    task_id: "00000000-0000-4000-8000-000000000001",
    created_at: "2026-07-15T00:00:00Z",
    payload: {
      task: {
        id: "00000000-0000-4000-8000-000000000001",
        repository_id: "00000000-0000-4000-8000-000000000002",
        client_request_id: "00000000-0000-4000-8000-000000000003",
        prompt: "test",
        status: "queued",
        delivery_readiness: "unreviewed",
        attempt: 1,
        last_event_id: id,
        created_at: "2026-07-15T00:00:00Z",
        retry_of: null,
        started_at: null,
        finished_at: null,
        failure: null,
      },
    },
  };
}

function lifecycleEvent(
  id: number,
  kind:
    | "task.queued"
    | "task.started"
    | "task.completed"
    | "task.failed"
    | "task.cancelled"
    | "task.interrupted",
  status:
    | "queued"
    | "running"
    | "completed"
    | "failed"
    | "cancelled"
    | "interrupted",
): unknown {
  const queued = taskQueued(id);
  const terminal = "2026-07-15T00:00:01Z";
  const taskByStatus = {
    queued: {
      ...queued.payload.task,
      status: "queued" as const,
      started_at: null,
      finished_at: null,
      failure: null,
    },
    running: {
      ...queued.payload.task,
      status: "running" as const,
      started_at: queued.payload.task.created_at,
      finished_at: null,
      failure: null,
    },
    completed: {
      ...queued.payload.task,
      status: "completed" as const,
      started_at: queued.payload.task.created_at,
      finished_at: terminal,
      failure: null,
    },
    failed: {
      ...queued.payload.task,
      status: "failed" as const,
      started_at: queued.payload.task.created_at,
      finished_at: terminal,
      failure: {
        code: "EXECUTOR_FAILED",
        message: "execution failed",
        retryable: false,
      },
    },
    cancelled: {
      ...queued.payload.task,
      status: "cancelled" as const,
      started_at: queued.payload.task.created_at,
      finished_at: terminal,
      failure: null,
    },
    interrupted: {
      ...queued.payload.task,
      status: "interrupted" as const,
      started_at: queued.payload.task.created_at,
      finished_at: terminal,
      failure: {
        code: "EXECUTOR_INTERRUPTED",
        message: "execution interrupted",
        retryable: true,
      },
    },
  };
  return {
    ...queued,
    kind,
    payload: {
      task: taskByStatus[status],
    },
  };
}

function reviewEvidence(): ReviewEvidence {
  const workspaceDigest = {
    algorithm: "workspace_fingerprint_v1" as const,
    value: "a".repeat(64),
  };
  const requiredCheck = {
    id: "check-cargo-test",
    kind: "cargo_test" as const,
    package: null,
    integration_test: null,
  };
  return {
    round: 1,
    decision_source: "reviewer",
    workspace_generation: 3,
    workspace_digest: workspaceDigest,
    verdict: "approved",
    summary: "Approved",
    findings: [],
    added_required_checks: [],
    required_checks: [requiredCheck],
    check_evidence: [
      {
        check_id: requiredCheck.id,
        actor: "executor",
        role_run: 1,
        workspace_generation: 3,
        workspace_digest: workspaceDigest,
        status: "passed",
        duration_ms: 12,
        summary: "passed",
        truncated: false,
      },
    ],
    coverage: {
      generation: 3,
      workspace_digest: workspaceDigest,
      manifest_sha256: "b".repeat(64),
      covered_chunks: [],
      total_chunks: 0,
    },
    created_at: "2026-07-15T00:00:00Z",
  };
}

function reviewUpdated(
  id: number,
): Extract<SseMessage, { kind: "review.updated" }> {
  return {
    id,
    schema_version: 1,
    kind: "review.updated",
    task_id: "00000000-0000-4000-8000-000000000001",
    created_at: "2026-07-15T00:00:00Z",
    payload: { review: reviewEvidence() },
  };
}

function panelEvent(
  id: number,
  kind: "plan.updated" | "activity.appended",
  payload: unknown,
): unknown {
  return {
    id,
    schema_version: 1,
    kind,
    task_id: "00000000-0000-4000-8000-000000000001",
    created_at: "2026-07-15T00:00:00Z",
    payload,
  };
}

function persistedFrame(
  id: number,
  body: unknown = taskQueued(id),
  event = "task.queued",
): string {
  return `id: ${id}\nevent: ${event}\ndata: ${JSON.stringify(body)}\n\n`;
}

const SCHEDULER_SERVER_INSTANCE_ID =
  "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const SCHEDULER_TASK_ID = "11111111-1111-4111-8111-111111111111";

function schedulerState(): SchedulerState {
  return {
    schema_version: 1,
    server_instance_id: SCHEDULER_SERVER_INSTANCE_ID,
    server_started_at: "2026-07-27T00:00:00Z",
    generation: 5,
    as_of_event_id: 7,
    service_state_generation: 3,
    admission_state: "running",
    limits: {
      global: 2,
      per_repository: 1,
      queued: 8,
      cargo_jobs_per_task: 2,
    },
    active_task_count: 0,
    queued_task_count: 1,
    queued_tasks: [
      { task_id: SCHEDULER_TASK_ID, reason: "global_capacity" },
    ],
    stopping_tasks: [],
    storage: {
      state: "normal",
      data: { state: "normal" },
      runtime: { state: "normal" },
      repositories: [],
    },
  };
}

async function schedulerControls(): Promise<{
  state: SchedulerState;
  manifest: SchedulerStateControl;
  chunk: SchedulerStateChunkControl;
}> {
  const state = schedulerState();
  const snapshotDigest = await schedulerStateDigest(state);
  return {
    state,
    manifest: {
      schema_version: 1,
      kind: "scheduler.state",
      server_instance_id: state.server_instance_id,
      server_started_at: state.server_started_at,
      generation: state.generation,
      as_of_event_id: state.as_of_event_id,
      service_state_generation: state.service_state_generation,
      admission_state: state.admission_state,
      limits: state.limits,
      active_task_count: state.active_task_count,
      queued_task_count: state.queued_task_count,
      stopping_task_count: state.stopping_tasks.length,
      repository_storage_count: state.storage.repositories.length,
      storage: {
        state: state.storage.state,
        data: state.storage.data,
        runtime: state.storage.runtime,
      },
      item_count: 1,
      chunk_count: 1,
      snapshot_digest: snapshotDigest,
    },
    chunk: {
      schema_version: 1,
      kind: "scheduler.state.chunk",
      server_instance_id: state.server_instance_id,
      generation: state.generation,
      snapshot_digest: snapshotDigest,
      chunk_index: 0,
      chunk_count: 1,
      items: [
        {
          kind: "queued_task",
          task_id: SCHEDULER_TASK_ID,
          reason: "global_capacity",
        },
      ],
    },
  };
}

function schedulerFrame(
  control: SchedulerStateControl | SchedulerStateChunkControl,
  id?: number,
): string {
  const idLine = id === undefined ? "" : `id: ${id}\n`;
  return `${idLine}event: ${control.kind}\ndata: ${JSON.stringify(control)}\n\n`;
}

function eventStream(
  chunks: readonly (string | Uint8Array)[],
  options: {
    onCancel?: () => void;
    holdOpen?: boolean;
    status?: number;
    contentType?: string;
    transportError?: Error;
  } = {},
): Response {
  let index = 0;
  return new Response(
    new ReadableStream<Uint8Array>({
      pull(controller) {
        const chunk = chunks[index++];
        if (chunk === undefined) {
          if (options.transportError !== undefined) {
            controller.error(options.transportError);
            return;
          }
          if (!options.holdOpen) {
            controller.close();
          }
          return;
        }
        controller.enqueue(typeof chunk === "string" ? encoder.encode(chunk) : chunk);
      },
      cancel() {
        options.onCancel?.();
      },
    }),
    {
      status: options.status ?? 200,
      headers: {
        "Content-Type": options.contentType ?? "text/event-stream; charset=utf-8",
      },
    },
  );
}

function callbacks(
  overrides: Partial<SseClientCallbacks> = {},
): SseClientCallbacks {
  return {
    onMessage: vi.fn(),
    onSchedulerSnapshot: vi.fn(),
    onDiagnostic: vi.fn(),
    onState: vi.fn(),
    recover: vi.fn(async () => 0),
    ...overrides,
  };
}

describe("IncrementalSseParser", () => {
  it("parses arbitrary UTF-8 chunks, mixed line endings, comments, and multiline data", () => {
    const input = encoder.encode(
      ": heartbeat\r\nevent: future.kind\rdata: first\ndata: 你🙂\n\r",
    );
    const parser = new IncrementalSseParser();
    const frames = [];

    for (const byte of input) {
      frames.push(...parser.push(Uint8Array.of(byte)));
    }
    frames.push(...parser.finish());

    expect(frames).toEqual([
      { event: "future.kind", data: "first\n你🙂" },
    ]);
  });

  it("rejects malformed UTF-8 in fatal mode", () => {
    const parser = new IncrementalSseParser();

    expect(() => parser.push(Uint8Array.of(0xc3, 0x28))).toThrowError(
      SseProtocolError,
    );
  });

  it("rejects an oversized frame before dispatch", () => {
    const parser = new IncrementalSseParser({ maxFrameBytes: 16 });

    expect(() => parser.push(encoder.encode("data: 12345678901\n\n"))).toThrow(
      /frame/i,
    );
  });

  it("enforces the default 256 KiB frame cap using UTF-8 bytes", () => {
    const parser = new IncrementalSseParser();

    expect(() =>
      parser.push(encoder.encode(`data: ${"🙂".repeat(65_536)}`)),
    ).toThrowError(
      expect.objectContaining({ code: "FRAME_TOO_LARGE" }),
    );
  });

  it("caps each frame rather than the total bytes in one transport chunk", () => {
    const parser = new IncrementalSseParser({ maxFrameBytes: 24 });

    const frames = parser.push(
      encoder.encode("data: one\n\ndata: two\n\ndata: three\n\n"),
    );

    expect(frames.map(({ data }) => data)).toEqual(["one", "two", "three"]);
  });

  it("rejects an EOF-truncated frame", () => {
    const parser = new IncrementalSseParser();
    parser.push(encoder.encode("event: task.queued\ndata: {}\n"));

    expect(() => parser.finish()).toThrow(/truncated/i);
  });

  it("treats a newline-terminated comment-only stream as a clean EOF", () => {
    const parser = new IncrementalSseParser();

    expect(parser.push(encoder.encode(": heartbeat\n"))).toEqual([]);
    expect(parser.finish()).toEqual([]);
  });
});

describe("computeBackoffDelay", () => {
  it("uses capped exponential backoff with injected jitter", () => {
    expect(computeBackoffDelay(0, 100, 1_000, () => 1)).toBe(100);
    expect(computeBackoffDelay(4, 100, 1_000, () => 1)).toBe(1_000);
    expect(computeBackoffDelay(4, 100, 1_000, () => 0)).toBe(500);
    expect(computeBackoffDelay(0, 1, 1, () => 0)).toBe(1);
  });
});

describe("SseClient", () => {
  it("fetches same-origin with cookies, Accept, redirect error, after cursor, and cleans the reader", async () => {
    let cancelled = false;
    const fetchMock = vi.fn(async () =>
      eventStream([persistedFrame(8)], {
        onCancel: () => (cancelled = true),
        holdOpen: true,
      }),
    );
    let client: SseClient;
    const cb = callbacks({
      onMessage: vi.fn(() => client.stop()),
    });
    client = new SseClient({ fetch: fetchMock, callbacks: cb });

    await client.start(7);

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/events?after=7",
      expect.objectContaining({
        credentials: "same-origin",
        redirect: "error",
        headers: { Accept: "text/event-stream" },
        signal: expect.any(AbortSignal),
      }),
    );
    expect(cb.onMessage).toHaveBeenCalledWith(taskQueued(8), {
      event: "task.queued",
      persistedId: 8,
    });
    expect(cancelled).toBe(true);
  });

  it("stops before projecting later frames from the same transport chunk", async () => {
    const seen: number[] = [];
    let client: SseClient;
    const cb = callbacks({
      onMessage: (message) => {
        if ("id" in message) {
          seen.push(message.id);
          client.stop();
        }
      },
    });
    client = new SseClient({
      fetch: vi.fn(async () =>
        eventStream([persistedFrame(8) + persistedFrame(9)]),
      ),
      callbacks: cb,
    });

    await client.start(7);

    expect(seen).toEqual([8]);
    expect(client.lastAppliedId).toBe(8);
  });

  it("does not publish open when an aborted pending fetch resolves late", async () => {
    let resolveResponse!: (response: Response) => void;
    const pendingResponse = new Promise<Response>((resolve) => {
      resolveResponse = resolve;
    });
    const states: SseClientState[] = [];
    let cancelled = false;
    const fetchMock = vi.fn(async () => pendingResponse);
    const client = new SseClient({
      fetch: fetchMock,
      callbacks: callbacks({ onState: (state) => states.push(state) }),
    });

    const running = client.start(7);
    expect(fetchMock).toHaveBeenCalledOnce();
    client.stop();
    resolveResponse(
      eventStream([], {
        holdOpen: true,
        onCancel: () => {
          cancelled = true;
        },
      }),
    );
    await running;

    expect(states.map((state) => state.kind)).toEqual(["connecting", "stopped"]);
    expect(cancelled).toBe(true);
  });

  it("projects service.state without advancing the persisted cursor", async () => {
    const received: Array<{ message: SseMessage; persistedId: number | null }> = [];
    let client: SseClient;
    const cb = callbacks({
      onMessage: (message, context) => {
        received.push({ message, persistedId: context.persistedId });
        if (message.kind === "task.queued") {
          client.stop();
        }
      },
    });
    client = new SseClient({
      fetch: vi.fn(async () =>
        eventStream([
          "event: service.state\ndata: {\"schema_version\":1,\"kind\":\"service.state\",\"state\":\"store_degraded\",\"generation\":4}\n\n",
          persistedFrame(8),
        ]),
      ),
      callbacks: cb,
    });

    await client.start(7);

    expect(received).toEqual([
      {
        message: {
          schema_version: 1,
          kind: "service.state",
          state: "store_degraded",
          generation: 4,
        },
        persistedId: null,
      },
      { message: taskQueued(8), persistedId: 8 },
    ]);
    expect(client.lastAppliedId).toBe(8);
  });

  it("assembles id-less scheduler controls and projects only the complete snapshot", async () => {
    const { state, manifest, chunk } = await schedulerControls();
    const candidates: SchedulerSnapshotCandidate[] = [];
    let client: SseClient;
    const onSchedulerSnapshot = vi.fn(
      (candidate: SchedulerSnapshotCandidate) => {
        candidates.push(candidate);
        client.stop();
      },
    );
    const cb = callbacks({
      onSchedulerSnapshot,
    });
    client = new SseClient({
      fetch: vi.fn(async () =>
        eventStream([schedulerFrame(manifest), schedulerFrame(chunk)]),
      ),
      callbacks: cb,
    });

    await client.start(7);

    expect(candidates).toEqual([
      expect.objectContaining({
        snapshot: state,
        digest: manifest.snapshot_digest,
      }),
    ]);
    expect(onSchedulerSnapshot).toHaveBeenCalledTimes(1);
    expect(cb.onMessage).not.toHaveBeenCalled();
    expect(client.lastAppliedId).toBe(7);
  });

  it.each(["clean EOF", "transport failure"] as const)(
    "recovers from the unchanged cursor when a partial scheduler snapshot meets %s",
    async (exitKind) => {
      const { manifest } = await schedulerControls();
      let client: SseClient;
      const recover = vi.fn(async () => {
        client.stop();
        return 20;
      });
      client = new SseClient({
        fetch: vi.fn(async () =>
          eventStream(
            [schedulerFrame(manifest)],
            exitKind === "transport failure"
              ? { transportError: new TypeError("connection lost") }
              : {},
          ),
        ),
        callbacks: callbacks({ recover }),
      });

      await client.start(7);

      expect(recover).toHaveBeenCalledWith(
        expect.objectContaining({
          code: "MALFORMED_ENVELOPE",
          message: expect.stringContaining(
            "before the scheduler snapshot completed",
          ),
        }),
        expect.any(AbortSignal),
      );
      expect(client.lastAppliedId).toBe(7);
    },
  );

  it("clears a partial scheduler snapshot on stream.reset and rejects its orphaned chunk after recovery", async () => {
    const { manifest, chunk } = await schedulerControls();
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        eventStream([
          schedulerFrame(manifest) +
            "event: stream.reset\ndata: {\"schema_version\":1,\"kind\":\"stream.reset\",\"latest_event_id\":7}\n\n",
        ]),
      )
      .mockResolvedValueOnce(eventStream([schedulerFrame(chunk)]));
    const reasons: SseRecoveryReason[] = [];
    let client: SseClient;
    const recover = vi.fn(async (reason: SseRecoveryReason) => {
      reasons.push(reason);
      if (reasons.length === 2) {
        client.stop();
      }
      return 7;
    });
    const cb = callbacks({ recover });
    client = new SseClient({
      fetch: fetchMock,
      callbacks: cb,
      baseDelayMs: 1,
      maxDelayMs: 1,
      jitter: () => 0,
      sleep: vi.fn(async () => undefined),
    });

    await client.start(7);

    expect(reasons.map(({ code }) => code)).toEqual([
      "STREAM_RESET",
      "MALFORMED_ENVELOPE",
    ]);
    expect(reasons[1]?.message).toContain("matching manifest");
    expect(cb.onSchedulerSnapshot).not.toHaveBeenCalled();
    expect(cb.onMessage).not.toHaveBeenCalled();
  });

  it("rejects ID-bearing scheduler controls before persisted event decoding", async () => {
    const { manifest, chunk } = await schedulerControls();

    for (const control of [manifest, chunk]) {
      let client: SseClient;
      const recover = vi.fn(async () => {
        client.stop();
        return 7;
      });
      client = new SseClient({
        fetch: vi.fn(async () => eventStream([schedulerFrame(control, 8)])),
        callbacks: callbacks({ recover }),
      });

      await client.start(7);

      expect(recover).toHaveBeenCalledWith(
        expect.objectContaining({
          code: "MALFORMED_ENVELOPE",
          message: expect.stringContaining("must be id-less"),
        }),
        expect.any(AbortSignal),
      );
    }
  });

  it("maps scheduler validator and assembler failures to protocol recovery", async () => {
    const { manifest, chunk } = await schedulerControls();
    const invalidManifest = { ...manifest, unexpected: true };

    for (const frame of [
      `event: scheduler.state\ndata: ${JSON.stringify(invalidManifest)}\n\n`,
      schedulerFrame(chunk),
    ]) {
      let client: SseClient;
      const recover = vi.fn(async () => {
        client.stop();
        return 7;
      });
      client = new SseClient({
        fetch: vi.fn(async () => eventStream([frame])),
        callbacks: callbacks({ recover }),
      });

      await client.start(7);

      expect(recover).toHaveBeenCalledWith(
        expect.objectContaining({ code: "MALFORMED_ENVELOPE" }),
        expect.any(AbortSignal),
      );
    }
  });

  it("maps a complete scheduler snapshot callback rejection to PROJECTION_ERROR", async () => {
    const { manifest, chunk } = await schedulerControls();
    let client: SseClient;
    const recover = vi.fn(async () => {
      client.stop();
      return 7;
    });
    client = new SseClient({
      fetch: vi.fn(async () =>
        eventStream([schedulerFrame(manifest), schedulerFrame(chunk)]),
      ),
      callbacks: callbacks({
        onSchedulerSnapshot: vi.fn(async () => {
          throw new Error("scheduler reducer rejected candidate");
        }),
        recover,
      }),
    });

    await client.start(7);

    expect(recover).toHaveBeenCalledWith(
      expect.objectContaining({ code: "PROJECTION_ERROR" }),
      expect.any(AbortSignal),
    );
    expect(client.lastAppliedId).toBe(7);
  });

  it("recognizes and projects a self-contained typed review.updated event", async () => {
    const onMessage = vi.fn();
    const recover = vi.fn(async () => 8);
    let client: SseClient;
    onMessage.mockImplementation(() => client.stop());
    client = new SseClient({
      fetch: vi.fn(async () =>
        eventStream([
          persistedFrame(8, reviewUpdated(8), "review.updated"),
        ]),
      ),
      callbacks: callbacks({ onMessage, recover }),
    });

    await client.start(7);

    expect(onMessage).toHaveBeenCalledWith(reviewUpdated(8), {
      event: "review.updated",
      persistedId: 8,
    });
    expect(client.lastAppliedId).toBe(8);
    expect(recover).not.toHaveBeenCalled();
  });

  it.each([
    [
      "task delivery_readiness",
      {
        ...taskQueued(8),
        payload: {
          task: {
            ...taskQueued(8).payload.task,
            delivery_readiness: undefined,
          },
        },
      },
      "task.queued",
    ],
    [
      "plan format_version",
      panelEvent(8, "plan.updated", {
        plan: {
          revision: 1,
          summary: "Implement the request",
          items: [
            {
              id: "step-1",
              title: "Implement",
              description: "Implement the requested change",
              acceptance_criteria: ["The focused tests pass"],
              status: "running",
            },
          ],
          initial_required_checks: [],
        },
      }),
      "plan.updated",
    ],
    [
      "activity role_run",
      panelEvent(8, "activity.appended", {
        entry: {
          id: "activity-1",
          level: "info",
          actor: "executor",
          message: "working",
          created_at: "2026-07-15T00:00:00Z",
        },
      }),
      "activity.appended",
    ],
    [
      "review coverage",
      {
        ...reviewUpdated(8),
        payload: {
          review: {
            ...reviewEvidence(),
            coverage: undefined,
          },
        },
      },
      "review.updated",
    ],
  ])("recovers when a known event omits required %s", async (_label, body, event) => {
    const recover = vi.fn(async () => {
      client.stop();
      return 8;
    });
    const client = new SseClient({
      fetch: vi.fn(async () => eventStream([persistedFrame(8, body, event)])),
      callbacks: callbacks({ recover }),
    });

    await client.start(7);

    expect(recover).toHaveBeenCalledWith(
      expect.objectContaining({ code: "MALFORMED_ENVELOPE" }),
      expect.any(AbortSignal),
    );
  });

  it("ignores a duplicate, diagnoses a supported future kind, advances, then recovers on non-monotonic IDs", async () => {
    const messages: SseMessage[] = [];
    const diagnostics: unknown[] = [];
    let client: SseClient;
    const recover = vi.fn(async () => {
      client.stop();
      return 50;
    });
    const future = {
      id: 12,
      schema_version: 1,
      kind: "task.future",
      task_id: "00000000-0000-4000-8000-000000000001",
      created_at: "2026-07-15T00:00:00Z",
      payload: {},
    };
    const response = eventStream([
      persistedFrame(11),
      persistedFrame(11),
      persistedFrame(12, future, "task.future"),
      persistedFrame(10, taskQueued(10)),
    ]);
    const cb = callbacks({
      onMessage: (message) => {
        messages.push(message);
      },
      onDiagnostic: (diagnostic) => {
        diagnostics.push(diagnostic);
      },
      recover,
    });
    client = new SseClient({ fetch: vi.fn(async () => response), callbacks: cb });

    await client.start(10);

    expect(messages).toEqual([taskQueued(11)]);
    expect(diagnostics).toEqual([
      expect.objectContaining({
        code: "UNKNOWN_EVENT_KIND",
        event: "task.future",
        persistedId: 12,
      }),
    ]);
    expect(client.lastAppliedId).toBe(12);
    expect(recover).toHaveBeenCalledWith(
      expect.objectContaining({ code: "NON_MONOTONIC_ID" }),
      expect.any(AbortSignal),
    );
  });

  it("does not commit an unknown-event cursor when its diagnostic callback rejects", async () => {
    let client: SseClient;
    const recover = vi.fn(async () => {
      client.stop();
      return 7;
    });
    const future = {
      id: 8,
      schema_version: 1,
      kind: "task.future",
      task_id: "00000000-0000-4000-8000-000000000001",
      created_at: "2026-07-15T00:00:00Z",
      payload: {},
    };
    client = new SseClient({
      fetch: vi.fn(async () =>
        eventStream([persistedFrame(8, future, "task.future")]),
      ),
      callbacks: callbacks({
        onDiagnostic: () => {
          throw new Error("projection rejected the diagnostic");
        },
        recover,
      }),
    });

    await client.start(7);

    expect(client.lastAppliedId).toBe(7);
    expect(recover).toHaveBeenCalledWith(
      expect.objectContaining({ code: "PROJECTION_ERROR" }),
      expect.any(AbortSignal),
    );
  });

  it.each([
    [
      "event/data kind disagreement",
      persistedFrame(8, taskQueued(8), "task.started"),
      "EVENT_KIND_MISMATCH",
    ],
    [
      "id disagreement",
      persistedFrame(8, taskQueued(9)),
      "EVENT_ID_MISMATCH",
    ],
    [
      "unsupported schema",
      persistedFrame(8, { ...taskQueued(8), schema_version: 2 }),
      "UNSUPPORTED_SCHEMA",
    ],
    [
      "malformed known payload",
      persistedFrame(8, { ...taskQueued(8), payload: {} }),
      "MALFORMED_ENVELOPE",
    ],
    [
      "id-bearing stream reset control",
      "id: 8\nevent: stream.reset\ndata: {\"schema_version\":1,\"kind\":\"stream.reset\",\"latest_event_id\":8}\n\n",
      "MALFORMED_ENVELOPE",
    ],
  ])("bootstraps after %s", async (_label, frame, code) => {
    let client: SseClient;
    const recover = vi.fn(async () => {
      client.stop();
      return 20;
    });
    client = new SseClient({
      fetch: vi.fn(async () => eventStream([frame])),
      callbacks: callbacks({ recover }),
    });

    await client.start(7);

    expect(recover).toHaveBeenCalledWith(
      expect.objectContaining({ code }),
      expect.any(AbortSignal),
    );
  });

  it.each([
    ["task.queued", "running"],
    ["task.started", "queued"],
    ["task.completed", "running"],
    ["task.failed", "running"],
    ["task.cancelled", "running"],
    ["task.interrupted", "running"],
  ] as const)(
    "rejects %s when payload.task has an inconsistent %s status",
    async (kind, wrongStatus) => {
      let client: SseClient;
      const recover = vi.fn(async () => {
        client.stop();
        return 8;
      });
      client = new SseClient({
        fetch: vi.fn(async () =>
          eventStream([persistedFrame(8, lifecycleEvent(8, kind, wrongStatus), kind)]),
        ),
        callbacks: callbacks({ recover }),
        sleep: async () => client.stop(),
      });

      await client.start(7);

      expect(recover).toHaveBeenCalledWith(
        expect.objectContaining({ code: "MALFORMED_ENVELOPE" }),
        expect.any(AbortSignal),
      );
    },
  );

  it.each([
    ["task.queued", "queued"],
    ["task.started", "running"],
    ["task.completed", "completed"],
    ["task.failed", "failed"],
    ["task.cancelled", "cancelled"],
    ["task.interrupted", "interrupted"],
  ] as const)(
    "accepts %s when payload.task status is %s and identifiers agree",
    async (kind, status) => {
      const onMessage = vi.fn();
      let client: SseClient;
      onMessage.mockImplementation(() => client.stop());
      const recover = vi.fn(async () => 8);
      client = new SseClient({
        fetch: vi.fn(async () =>
          eventStream([persistedFrame(8, lifecycleEvent(8, kind, status), kind)]),
        ),
        callbacks: callbacks({ onMessage, recover }),
      });

      await client.start(7);

      expect(onMessage).toHaveBeenCalledWith(
        expect.objectContaining({ kind, payload: { task: expect.objectContaining({ status }) } }),
        { event: kind, persistedId: 8 },
      );
      expect(recover).not.toHaveBeenCalled();
    },
  );

  it.each([
    [
      "task id",
      {
        ...taskQueued(8),
        payload: {
          task: {
            ...taskQueued(8).payload.task,
            id: "00000000-0000-4000-8000-000000000099",
          },
        },
      },
    ],
    [
      "last event id",
      {
        ...taskQueued(8),
        payload: {
          task: { ...taskQueued(8).payload.task, last_event_id: 7 },
        },
      },
    ],
  ])("rejects a lifecycle payload with a mismatched %s", async (_label, body) => {
    let client: SseClient;
    const recover = vi.fn(async () => {
      client.stop();
      return 8;
    });
    client = new SseClient({
      fetch: vi.fn(async () => eventStream([persistedFrame(8, body)])),
      callbacks: callbacks({ recover }),
      sleep: async () => client.stop(),
    });

    await client.start(7);

    expect(recover).toHaveBeenCalledWith(
      expect.objectContaining({ code: "MALFORMED_ENVELOPE" }),
      expect.any(AbortSignal),
    );
  });

  it("uses bootstrap cursor after stream.reset before reopening", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        eventStream([
          "event: stream.reset\ndata: {\"schema_version\":1,\"kind\":\"stream.reset\",\"latest_event_id\":40}\n\n",
        ]),
      )
      .mockResolvedValueOnce(eventStream([persistedFrame(42)]));
    let client: SseClient;
    const recover = vi.fn(async () => 41);
    const cb = callbacks({
      recover,
      onMessage: vi.fn(() => client.stop()),
    });
    client = new SseClient({ fetch: fetchMock, callbacks: cb });

    await client.start(7);

    expect(recover).toHaveBeenCalledWith(
      expect.objectContaining({ code: "STREAM_RESET" }),
      expect.any(AbortSignal),
    );
    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      "/api/events?after=7",
      "/api/events?after=41",
    ]);
  });

  it("enters session-expired on 401 and never reconnects", async () => {
    const states: SseClientState[] = [];
    const sleep = vi.fn(async () => undefined);
    let cancelled = false;
    const fetchMock = vi.fn(async () =>
      eventStream(["ignored"], {
        status: 401,
        holdOpen: true,
        onCancel: () => (cancelled = true),
      }),
    );
    const cb = callbacks({ onState: (state) => states.push(state) });
    const client = new SseClient({ fetch: fetchMock, callbacks: cb, sleep });

    await client.start(3);

    expect(states.at(-1)).toEqual({ kind: "session-expired" });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
    expect(cb.recover).not.toHaveBeenCalled();
    expect(cancelled).toBe(true);
  });

  it("cancels an invalid-content-type body before bootstrap recovery", async () => {
    let cancelled = false;
    let client: SseClient;
    const recover = vi.fn(async () => {
      client.stop();
      return 0;
    });
    client = new SseClient({
      fetch: vi.fn(async () =>
        eventStream(["not sse"], {
          contentType: "application/json",
          holdOpen: true,
          onCancel: () => (cancelled = true),
        }),
      ),
      callbacks: callbacks({ recover }),
    });

    await client.start(0);

    expect(cancelled).toBe(true);
    expect(recover).toHaveBeenCalledWith(
      expect.objectContaining({ code: "INVALID_CONTENT_TYPE" }),
      expect.any(AbortSignal),
    );
  });

  it.each([
    ["clean EOF", eventStream([])],
    ["503", new Response(null, { status: 503 })],
  ])("reconnects after %s from the unchanged cursor", async (_label, first) => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(first)
      .mockResolvedValueOnce(eventStream([persistedFrame(10)]));
    const delays: number[] = [];
    let client: SseClient;
    const cb = callbacks({ onMessage: vi.fn(() => client.stop()) });
    client = new SseClient({
      fetch: fetchMock,
      callbacks: cb,
      baseDelayMs: 100,
      jitter: () => 1,
      sleep: async (delay) => {
        delays.push(delay);
      },
    });

    await client.start(9);

    expect(delays).toEqual([100]);
    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      "/api/events?after=9",
      "/api/events?after=9",
    ]);
  });

  it("treats a fetch rejection as transport reconnect from the same cursor", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockRejectedValueOnce(new TypeError("redirect or network failure"))
      .mockResolvedValueOnce(eventStream([persistedFrame(10)]));
    let client: SseClient;
    const cb = callbacks({ onMessage: vi.fn(() => client.stop()) });
    client = new SseClient({
      fetch: fetchMock,
      callbacks: cb,
      baseDelayMs: 25,
      jitter: () => 1,
      sleep: vi.fn(async () => undefined),
    });

    await client.start(9);

    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      "/api/events?after=9",
      "/api/events?after=9",
    ]);
  });

  it("backs off after every successful recovery from a persistent protocol error", async () => {
    const delays: number[] = [];
    let client: SseClient;
    let recoveryCount = 0;
    const recover = vi.fn(async () => {
      recoveryCount += 1;
      if (recoveryCount === 4) {
        client.stop();
      }
      return 7;
    });
    const fetchMock = vi.fn(async () =>
      eventStream(["event: task.queued\nid: 8\ndata: {\n\n"]),
    );
    client = new SseClient({
      fetch: fetchMock,
      callbacks: callbacks({ recover }),
      baseDelayMs: 100,
      maxDelayMs: 200,
      jitter: () => 1,
      sleep: async (delay) => {
        delays.push(delay);
      },
    });

    await client.start(7);

    expect(fetchMock).toHaveBeenCalledTimes(4);
    expect(delays).toEqual([100, 200, 200]);
  });

  it("does not reset the reconnect failure streak for service or duplicate frames", async () => {
    const serviceState =
      "event: service.state\ndata: {\"schema_version\":1,\"kind\":\"service.state\",\"state\":\"ready\",\"generation\":2}\n\n";
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(eventStream([]))
      .mockResolvedValueOnce(eventStream([serviceState]))
      .mockResolvedValueOnce(eventStream([persistedFrame(7)]))
      .mockResolvedValueOnce(eventStream([persistedFrame(8)]));
    const delays: number[] = [];
    let client: SseClient;
    client = new SseClient({
      fetch: fetchMock,
      callbacks: callbacks({
        onMessage: (message) => {
          if ("id" in message && message.id === 8) {
            client.stop();
          }
        },
      }),
      baseDelayMs: 100,
      maxDelayMs: 1_000,
      jitter: () => 1,
      sleep: async (delay) => {
        delays.push(delay);
      },
    });

    await client.start(7);

    expect(delays).toEqual([100, 200, 400]);
  });

  it("resets the failure streak only after the persisted cursor advances", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(eventStream([]))
      .mockResolvedValueOnce(eventStream([persistedFrame(8)]))
      .mockResolvedValueOnce(eventStream([persistedFrame(9)]));
    const delays: number[] = [];
    let client: SseClient;
    client = new SseClient({
      fetch: fetchMock,
      callbacks: callbacks({
        onMessage: (message) => {
          if ("id" in message && message.id === 9) {
            client.stop();
          }
        },
      }),
      baseDelayMs: 100,
      jitter: () => 1,
      sleep: async (delay) => {
        delays.push(delay);
      },
    });

    await client.start(7);

    expect(delays).toEqual([100, 100]);
  });

  it("still waits at least base delay when recovery bootstrap advances the cursor", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(eventStream([]))
      .mockResolvedValueOnce(
        eventStream(["event: task.queued\nid: 8\ndata: {\n\n"]),
      )
      .mockResolvedValueOnce(eventStream([persistedFrame(11)]));
    const delays: number[] = [];
    let client: SseClient;
    client = new SseClient({
      fetch: fetchMock,
      callbacks: callbacks({
        recover: vi.fn(async () => 10),
        onMessage: () => client.stop(),
      }),
      baseDelayMs: 100,
      jitter: () => 1,
      sleep: async (delay) => {
        delays.push(delay);
      },
    });

    await client.start(7);

    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      "/api/events?after=7",
      "/api/events?after=7",
      "/api/events?after=10",
    ]);
    expect(delays).toEqual([100, 100]);
  });

  it.each([
    ["malformed JSON", "event: task.queued\nid: 8\ndata: {\n\n", "MALFORMED_JSON"],
    ["EOF partial", "event: task.queued\ndata: {}\n", "TRUNCATED_FRAME"],
    ["oversized frame", `data: ${"x".repeat(128)}\n\n`, "FRAME_TOO_LARGE"],
  ])("bootstraps after %s", async (_label, input, code) => {
    let client: SseClient;
    const recover = vi.fn(async () => {
      client.stop();
      return 30;
    });
    client = new SseClient({
      fetch: vi.fn(async () => eventStream([input])),
      callbacks: callbacks({ recover }),
      maxFrameBytes: 64,
    });

    await client.start(7);

    expect(recover).toHaveBeenCalledWith(
      expect.objectContaining({ code }),
      expect.any(AbortSignal),
    );
  });

  it("shows unavailable state and retries a failed bootstrap with capped backoff", async () => {
    const states: SseClientState[] = [];
    const delays: number[] = [];
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response("no", { status: 200 }))
      .mockResolvedValueOnce(eventStream([persistedFrame(22)]));
    const recover = vi
      .fn<SseClientCallbacks["recover"]>()
      .mockRejectedValueOnce(new Error("offline"))
      .mockRejectedValueOnce(new Error("still offline"))
      .mockResolvedValueOnce(21);
    let client: SseClient;
    const cb = callbacks({
      recover,
      onState: (state) => states.push(state),
      onMessage: vi.fn(() => client.stop()),
    });
    client = new SseClient({
      fetch: fetchMock,
      callbacks: cb,
      baseDelayMs: 100,
      maxDelayMs: 200,
      jitter: () => 1,
      sleep: async (delay) => {
        delays.push(delay);
      },
    });

    await client.start(5);

    expect(delays).toEqual([100, 200, 100]);
    expect(states.filter(({ kind }) => kind === "unavailable")).toHaveLength(2);
    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      "/api/events?after=5",
      "/api/events?after=21",
    ]);
  });

  it("merges an explicit recovery requested during bootstrap backoff and only exits on stop", async () => {
    let resolveBackoffStarted!: (signal: AbortSignal) => void;
    const backoffStarted = new Promise<AbortSignal>((resolve) => {
      resolveBackoffStarted = resolve;
    });
    let resolveSecondRecovery!: (value: {
      reason: SseRecoveryReason;
      signal: AbortSignal;
    }) => void;
    const secondRecovery = new Promise<{
      reason: SseRecoveryReason;
      signal: AbortSignal;
    }>((resolve) => {
      resolveSecondRecovery = resolve;
    });
    const recover = vi
      .fn<SseClientCallbacks["recover"]>()
      .mockRejectedValueOnce(new Error("bootstrap offline"))
      .mockImplementationOnce((reason, signal) => {
        resolveSecondRecovery({ reason, signal });
        return new Promise<number>((_resolve, reject) => {
          signal.addEventListener(
            "abort",
            () => reject(new DOMException("bootstrap aborted", "AbortError")),
            { once: true },
          );
        });
      });
    const sleep = vi.fn((_delay: number, signal: AbortSignal) => {
      resolveBackoffStarted(signal);
      return new Promise<void>((_resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => reject(new DOMException("backoff aborted", "AbortError")),
          { once: true },
        );
      });
    });
    const client = new SseClient({
      fetch: vi.fn(async () => new Response("not an event stream")),
      callbacks: callbacks({ recover }),
      sleep,
      baseDelayMs: 1,
      maxDelayMs: 1,
      jitter: () => 0,
    });

    const running = client.start(5);
    await backoffStarted;
    client.requestRecovery({
      code: "PROJECTION_ERROR",
      message: "buffered review replay conflicted",
    });

    const outcome = await Promise.race([
      secondRecovery.then((value) => ({ kind: "second" as const, value })),
      running.then(() => ({ kind: "stopped" as const })),
    ]);
    expect(outcome.kind).toBe("second");
    if (outcome.kind !== "second") return;
    expect(outcome.value.reason.code).toBe("PROJECTION_ERROR");
    expect(outcome.value.reason.message).toContain(
      "expected text/event-stream",
    );
    expect(outcome.value.reason.message).toContain(
      "buffered review replay conflicted",
    );
    expect(recover).toHaveBeenCalledTimes(2);
    expect(sleep).toHaveBeenCalledTimes(1);

    let settled = false;
    void running.then(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);

    client.stop();
    await running;
    expect(settled).toBe(true);
  });

  it("treats a recovery-bootstrap 401 as session expired without retry", async () => {
    const states: SseClientState[] = [];
    const sleep = vi.fn(async () => undefined);
    const recover = vi.fn(async () => {
      throw Object.assign(new Error("session expired"), { status: 401 });
    });
    const fetchMock = vi.fn(async () =>
      eventStream(["event: task.queued\nid: 8\ndata: {\n\n"]),
    );
    const client = new SseClient({
      fetch: fetchMock,
      callbacks: callbacks({
        recover,
        onState: (state) => states.push(state),
      }),
      sleep,
    });

    await client.start(7);

    expect(recover).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
    expect(states.at(-1)).toEqual({ kind: "session-expired" });
  });
});
