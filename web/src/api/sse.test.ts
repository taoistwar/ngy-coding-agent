import { describe, expect, it, vi } from "vitest";

import type { SseMessage } from "./types";
import {
  IncrementalSseParser,
  SseClient,
  SseProtocolError,
  computeBackoffDelay,
  type SseClientCallbacks,
  type SseClientState,
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
        attempt: 1,
        last_event_id: id,
        created_at: "2026-07-15T00:00:00Z",
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
  return {
    ...queued,
    kind,
    payload: {
      task: { ...queued.payload.task, status },
    },
  };
}

function persistedFrame(
  id: number,
  body: unknown = taskQueued(id),
  event = "task.queued",
): string {
  return `id: ${id}\nevent: ${event}\ndata: ${JSON.stringify(body)}\n\n`;
}

function eventStream(
  chunks: readonly (string | Uint8Array)[],
  options: {
    onCancel?: () => void;
    holdOpen?: boolean;
    status?: number;
    contentType?: string;
  } = {},
): Response {
  let index = 0;
  return new Response(
    new ReadableStream<Uint8Array>({
      pull(controller) {
        const chunk = chunks[index++];
        if (chunk === undefined) {
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
