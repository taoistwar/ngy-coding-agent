import type { SseMessage, Task, TaskEvent } from "./types";

const DEFAULT_ENDPOINT = "/api/events";
const DEFAULT_MAX_FRAME_BYTES = 256 * 1024;
const DEFAULT_BASE_DELAY_MS = 250;
const DEFAULT_MAX_DELAY_MS = 10_000;
const SUPPORTED_SCHEMA_VERSION = 1;

const TASK_EVENT_KINDS = new Set([
  "task.queued",
  "task.started",
  "plan.updated",
  "activity.appended",
  "diff.updated",
  "test.updated",
  "task.completed",
  "task.failed",
  "task.cancelled",
  "task.interrupted",
] as const);

export type SseProtocolCode =
  | "MALFORMED_UTF8"
  | "FRAME_TOO_LARGE"
  | "TRUNCATED_FRAME"
  | "MALFORMED_JSON"
  | "MALFORMED_ENVELOPE"
  | "UNSUPPORTED_SCHEMA"
  | "EVENT_KIND_MISMATCH"
  | "EVENT_ID_MISMATCH"
  | "NON_MONOTONIC_ID"
  | "STREAM_RESET"
  | "INVALID_CONTENT_TYPE"
  | "HTTP_ERROR"
  | "PROJECTION_ERROR";

export class SseProtocolError extends Error {
  readonly code: SseProtocolCode;

  constructor(code: SseProtocolCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "SseProtocolError";
    this.code = code;
  }
}

export interface SseFrame {
  readonly event: string;
  readonly data: string;
  readonly id?: string;
}

export interface IncrementalSseParserOptions {
  readonly maxFrameBytes?: number;
}

/**
 * A deliberately small SSE parser. It owns UTF-8 decoding so malformed input
 * cannot be replaced with U+FFFD before protocol validation sees it.
 */
export class IncrementalSseParser {
  readonly #decoder = new TextDecoder("utf-8", { fatal: true });
  readonly #encoder = new TextEncoder();
  readonly #maxFrameBytes: number;
  #pending = "";
  #dataLines: string[] = [];
  #eventName = "";
  #eventId: string | undefined;
  #frameBytes = 0;
  #finished = false;

  constructor(options: IncrementalSseParserOptions = {}) {
    const maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
    if (!Number.isSafeInteger(maxFrameBytes) || maxFrameBytes <= 0) {
      throw new RangeError("maxFrameBytes must be a positive safe integer");
    }
    this.#maxFrameBytes = maxFrameBytes;
  }

  push(chunk: Uint8Array): SseFrame[] {
    if (this.#finished) {
      throw new Error("cannot push after the SSE parser is finished");
    }

    try {
      this.#pending += this.#decoder.decode(chunk, { stream: true });
    } catch (error) {
      throw new SseProtocolError(
        "MALFORMED_UTF8",
        "the SSE stream contained malformed UTF-8",
        { cause: error },
      );
    }

    const frames = this.#drainLines(false);
    this.#assertFrameSize();
    return frames;
  }

  finish(): SseFrame[] {
    if (this.#finished) {
      return [];
    }
    this.#finished = true;

    try {
      this.#pending += this.#decoder.decode();
    } catch (error) {
      throw new SseProtocolError(
        "MALFORMED_UTF8",
        "the SSE stream ended inside a UTF-8 sequence",
        { cause: error },
      );
    }

    const frames = this.#drainLines(true);
    this.#assertFrameSize();
    if (
      this.#pending.length > 0 ||
      this.#frameBytes > 0 ||
      this.#dataLines.length > 0 ||
      this.#eventName.length > 0 ||
      this.#eventId !== undefined
    ) {
      throw new SseProtocolError(
        "TRUNCATED_FRAME",
        "the SSE stream ended with a truncated frame before its blank line",
      );
    }
    return frames;
  }

  #drainLines(finishing: boolean): SseFrame[] {
    const frames: SseFrame[] = [];

    while (true) {
      const lineEnding = findLineEnding(this.#pending);
      if (lineEnding === undefined) {
        break;
      }
      if (
        lineEnding.character === "\r" &&
        lineEnding.index === this.#pending.length - 1 &&
        !finishing
      ) {
        break;
      }

      const isCrLf =
        lineEnding.character === "\r" &&
        this.#pending[lineEnding.index + 1] === "\n";
      const separatorLength = isCrLf ? 2 : 1;
      const line = this.#pending.slice(0, lineEnding.index);
      this.#pending = this.#pending.slice(lineEnding.index + separatorLength);
      this.#frameBytes +=
        this.#encoder.encode(line).byteLength + separatorLength;
      if (this.#frameBytes > this.#maxFrameBytes) {
        this.#throwFrameTooLarge();
      }

      const frame = this.#consumeLine(line);
      if (frame !== undefined) {
        frames.push(frame);
      }
    }

    return frames;
  }

  #consumeLine(line: string): SseFrame | undefined {
    if (line.length === 0) {
      const frame =
        this.#dataLines.length === 0
          ? undefined
          : this.#makeFrame(this.#dataLines.join("\n"));
      this.#dataLines = [];
      this.#eventName = "";
      this.#eventId = undefined;
      this.#frameBytes = 0;
      return frame;
    }

    if (line.startsWith(":")) {
      return undefined;
    }

    const colon = line.indexOf(":");
    const field = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) {
      value = value.slice(1);
    }

    switch (field) {
      case "data":
        this.#dataLines.push(value);
        break;
      case "event":
        this.#eventName = value;
        break;
      case "id":
        if (!value.includes("\0")) {
          this.#eventId = value;
        }
        break;
      default:
        // `retry` and future fields are transport hints, not application data.
        break;
    }
    return undefined;
  }

  #makeFrame(data: string): SseFrame {
    const base = {
      event: this.#eventName.length === 0 ? "message" : this.#eventName,
      data,
    };
    return this.#eventId === undefined ? base : { ...base, id: this.#eventId };
  }

  #assertFrameSize(): void {
    const bufferedBytes =
      this.#frameBytes + this.#encoder.encode(this.#pending).byteLength;
    if (bufferedBytes > this.#maxFrameBytes) {
      this.#throwFrameTooLarge();
    }
  }

  #throwFrameTooLarge(): never {
    throw new SseProtocolError(
      "FRAME_TOO_LARGE",
      `SSE frame exceeded ${this.#maxFrameBytes} bytes`,
    );
  }
}

interface LineEnding {
  readonly index: number;
  readonly character: "\r" | "\n";
}

function findLineEnding(value: string): LineEnding | undefined {
  for (let index = 0; index < value.length; index += 1) {
    const character = value[index];
    if (character === "\r" || character === "\n") {
      return { index, character };
    }
  }
  return undefined;
}

export interface SseRecoveryReason {
  readonly code: SseProtocolCode;
  readonly message: string;
}

export interface SseDiagnostic {
  readonly code: "UNKNOWN_EVENT_KIND";
  readonly event: string;
  readonly persistedId: number;
  readonly message: string;
}

export interface SseMessageContext {
  readonly event: string;
  readonly persistedId: number | null;
}

export type SseClientState =
  | { readonly kind: "connecting"; readonly cursor: number }
  | { readonly kind: "open"; readonly cursor: number }
  | {
      readonly kind: "reconnecting";
      readonly cursor: number;
      readonly attempt: number;
      readonly delayMs: number;
      readonly reason:
        | "clean-eof"
        | "transport"
        | "http-transient"
        | "protocol-recovered";
    }
  | {
      readonly kind: "recovering";
      readonly cursor: number;
      readonly reason: SseRecoveryReason;
    }
  | {
      readonly kind: "unavailable";
      readonly attempt: number;
      readonly delayMs: number;
      readonly reason: SseRecoveryReason;
      readonly error: unknown;
    }
  | { readonly kind: "session-expired" }
  | { readonly kind: "stopped" };

export interface SseClientCallbacks {
  readonly onMessage: (
    message: SseMessage,
    context: SseMessageContext,
  ) => void | Promise<void>;
  readonly onDiagnostic: (
    diagnostic: SseDiagnostic,
  ) => void | Promise<void>;
  readonly onState: (state: SseClientState) => void;
  /**
   * Perform and install a full bootstrap, then return its persisted event
   * cursor. A rejected promise keeps the stream closed and enters capped
   * bootstrap retry.
   */
  readonly recover: (
    reason: SseRecoveryReason,
    signal: AbortSignal,
  ) => Promise<number>;
}

export interface SseClientOptions {
  readonly callbacks: SseClientCallbacks;
  readonly endpoint?: string;
  readonly fetch?: typeof globalThis.fetch;
  readonly sleep?: (delayMs: number, signal: AbortSignal) => Promise<void>;
  readonly jitter?: () => number;
  readonly baseDelayMs?: number;
  readonly maxDelayMs?: number;
  readonly maxFrameBytes?: number;
  readonly schemaVersion?: number;
}

type TransientReason = Extract<
  SseClientState,
  { kind: "reconnecting" }
>["reason"];

type ConnectionExit =
  | { readonly kind: "stopped" }
  | { readonly kind: "session-expired" }
  | {
      readonly kind: "transient";
      readonly reason: TransientReason;
      readonly cursorAdvanced: boolean;
    }
  | {
      readonly kind: "protocol";
      readonly reason: SseRecoveryReason;
      readonly cursorAdvanced: boolean;
    };

export class SseClient {
  readonly #callbacks: SseClientCallbacks;
  readonly #endpoint: string;
  readonly #fetch: typeof globalThis.fetch;
  readonly #sleep: (delayMs: number, signal: AbortSignal) => Promise<void>;
  readonly #jitter: () => number;
  readonly #baseDelayMs: number;
  readonly #maxDelayMs: number;
  readonly #maxFrameBytes: number;
  readonly #schemaVersion: number;
  #lastAppliedId = 0;
  #stopped = true;
  #running = false;
  #activeController: AbortController | undefined;

  constructor(options: SseClientOptions) {
    this.#callbacks = options.callbacks;
    this.#endpoint = options.endpoint ?? DEFAULT_ENDPOINT;
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.#sleep = options.sleep ?? abortableSleep;
    this.#jitter = options.jitter ?? Math.random;
    this.#baseDelayMs = positiveDelay(
      options.baseDelayMs ?? DEFAULT_BASE_DELAY_MS,
      "baseDelayMs",
    );
    this.#maxDelayMs = positiveDelay(
      options.maxDelayMs ?? DEFAULT_MAX_DELAY_MS,
      "maxDelayMs",
    );
    if (this.#baseDelayMs > this.#maxDelayMs) {
      throw new RangeError("baseDelayMs must not exceed maxDelayMs");
    }
    this.#maxFrameBytes = positiveDelay(
      options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES,
      "maxFrameBytes",
    );
    this.#schemaVersion = positiveDelay(
      options.schemaVersion ?? SUPPORTED_SCHEMA_VERSION,
      "schemaVersion",
    );
  }

  get lastAppliedId(): number {
    return this.#lastAppliedId;
  }

  async start(cursor: number): Promise<void> {
    if (this.#running) {
      throw new Error("SSE client is already running");
    }
    assertCursor(cursor);
    this.#running = true;
    this.#stopped = false;
    this.#lastAppliedId = cursor;

    try {
      await this.#run();
    } finally {
      this.#activeController?.abort();
      this.#activeController = undefined;
      this.#running = false;
    }
  }

  stop(): void {
    this.#stopped = true;
    this.#activeController?.abort();
  }

  async #run(): Promise<void> {
    let failureStreak = 0;

    while (!this.#stopped) {
      this.#callbacks.onState({
        kind: "connecting",
        cursor: this.#lastAppliedId,
      });
      const exit = await this.#openOnce();

      if (exit.kind === "stopped") {
        break;
      }
      if (exit.kind === "session-expired") {
        this.#stopped = true;
        this.#callbacks.onState({ kind: "session-expired" });
        return;
      }
      if (exit.cursorAdvanced) {
        failureStreak = 0;
      }
      if (exit.kind === "protocol") {
        const cursorBeforeRecovery = this.#lastAppliedId;
        const recovery = await this.#recover(exit.reason);
        if (recovery === "session-expired") {
          this.#stopped = true;
          this.#callbacks.onState({ kind: "session-expired" });
          return;
        }
        if (recovery === "stopped") {
          break;
        }
        if (this.#lastAppliedId > cursorBeforeRecovery) {
          failureStreak = 0;
        }
        const delayMs = Math.max(
          this.#baseDelayMs,
          computeBackoffDelay(
            failureStreak,
            this.#baseDelayMs,
            this.#maxDelayMs,
            this.#jitter,
          ),
        );
        failureStreak += 1;
        this.#callbacks.onState({
          kind: "reconnecting",
          cursor: this.#lastAppliedId,
          attempt: failureStreak,
          delayMs,
          reason: "protocol-recovered",
        });
        if (!(await this.#wait(delayMs))) {
          break;
        }
        continue;
      }

      const delayMs = computeBackoffDelay(
        failureStreak,
        this.#baseDelayMs,
        this.#maxDelayMs,
        this.#jitter,
      );
      failureStreak += 1;
      this.#callbacks.onState({
        kind: "reconnecting",
        cursor: this.#lastAppliedId,
        attempt: failureStreak,
        delayMs,
        reason: exit.reason,
      });
      if (!(await this.#wait(delayMs))) {
        break;
      }
    }

    this.#callbacks.onState({ kind: "stopped" });
  }

  async #openOnce(): Promise<ConnectionExit> {
    const controller = new AbortController();
    this.#activeController = controller;
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    const cursorAtOpen = this.#lastAppliedId;
    const cursorAdvanced = () => this.#lastAppliedId > cursorAtOpen;

    try {
      let response: Response;
      try {
        response = await this.#fetch(
          withCursor(this.#endpoint, this.#lastAppliedId),
          {
            method: "GET",
            credentials: "same-origin",
            redirect: "error",
            headers: { Accept: "text/event-stream" },
            signal: controller.signal,
          },
        );
      } catch {
        return this.#stopped
          ? { kind: "stopped" }
          : {
              kind: "transient",
              reason: "transport",
              cursorAdvanced: false,
            };
      }

      if (this.#stopped) {
        await cancelResponseBody(response);
        return { kind: "stopped" };
      }

      if (response.status === 401) {
        await cancelResponseBody(response);
        return { kind: "session-expired" };
      }
      if (!response.ok) {
        await cancelResponseBody(response);
        if (isTransientStatus(response.status)) {
          return {
            kind: "transient",
            reason: "http-transient",
            cursorAdvanced: false,
          };
        }
        return {
          kind: "protocol",
          reason: {
            code: "HTTP_ERROR",
            message: `event stream returned non-success status ${response.status}`,
          },
          cursorAdvanced: false,
        };
      }

      const contentType = response.headers.get("Content-Type");
      if (!isEventStreamContentType(contentType)) {
        await cancelResponseBody(response);
        return {
          kind: "protocol",
          reason: {
            code: "INVALID_CONTENT_TYPE",
            message: `expected text/event-stream, received ${contentType ?? "no content type"}`,
          },
          cursorAdvanced: false,
        };
      }
      if (response.body === null) {
        return {
          kind: "protocol",
          reason: {
            code: "MALFORMED_ENVELOPE",
            message: "event stream response did not contain a body",
          },
          cursorAdvanced: false,
        };
      }

      reader = response.body.getReader();
      this.#callbacks.onState({
        kind: "open",
        cursor: this.#lastAppliedId,
      });
      const parser = new IncrementalSseParser({
        maxFrameBytes: this.#maxFrameBytes,
      });

      while (!this.#stopped) {
        let result: ReadableStreamReadResult<Uint8Array>;
        try {
          result = await reader.read();
        } catch {
          return this.#stopped
            ? { kind: "stopped" }
            : {
                kind: "transient",
                reason: "transport",
              cursorAdvanced: cursorAdvanced(),
            };
        }

        if (this.#stopped) {
          return { kind: "stopped" };
        }

        if (result.done) {
          try {
            const finalFrames = parser.finish();
            for (const frame of finalFrames) {
              if (this.#stopped) {
                return { kind: "stopped" };
              }
              await this.#applyFrame(frame);
              if (this.#stopped) {
                return { kind: "stopped" };
              }
            }
          } catch (error) {
            if (this.#stopped) {
              return { kind: "stopped" };
            }
            return protocolExit(error, cursorAdvanced());
          }
          return {
            kind: "transient",
            reason: "clean-eof",
            cursorAdvanced: cursorAdvanced(),
          };
        }

        try {
          const frames = parser.push(result.value);
          for (const frame of frames) {
            if (this.#stopped) {
              return { kind: "stopped" };
            }
            await this.#applyFrame(frame);
            if (this.#stopped) {
              return { kind: "stopped" };
            }
          }
        } catch (error) {
          if (this.#stopped) {
            return { kind: "stopped" };
          }
          return protocolExit(error, cursorAdvanced());
        }
      }
      return { kind: "stopped" };
    } finally {
      if (reader !== undefined) {
        try {
          await reader.cancel();
        } catch {
          // Cleanup failure does not change the already-classified exit.
        }
        try {
          reader.releaseLock();
        } catch {
          // The stream can already have released its lock after cancellation.
        }
      }
      controller.abort();
      if (this.#activeController === controller) {
        this.#activeController = undefined;
      }
    }
  }

  async #applyFrame(frame: SseFrame): Promise<void> {
    const decoded = decodeFrame(frame, this.#schemaVersion);

    if (decoded.kind === "reset") {
      throw new SseProtocolError(
        "STREAM_RESET",
        `server requested a stream reset at event ${decoded.latestEventId}`,
      );
    }
    if (decoded.persistedId !== null) {
      if (decoded.persistedId === this.#lastAppliedId) {
        return;
      }
      if (decoded.persistedId < this.#lastAppliedId) {
        throw new SseProtocolError(
          "NON_MONOTONIC_ID",
          `persisted event ${decoded.persistedId} followed cursor ${this.#lastAppliedId}`,
        );
      }
    }

    if (decoded.kind === "diagnostic") {
      this.#lastAppliedId = decoded.persistedId;
      await this.#callbacks.onDiagnostic(decoded.diagnostic);
      return;
    }

    try {
      await this.#callbacks.onMessage(decoded.message, {
        event: frame.event,
        persistedId: decoded.persistedId,
      });
    } catch (error) {
      throw new SseProtocolError(
        "PROJECTION_ERROR",
        "event projection callback rejected a validated SSE message",
        { cause: error },
      );
    }
    if (decoded.persistedId !== null) {
      this.#lastAppliedId = decoded.persistedId;
    }
  }

  async #recover(
    reason: SseRecoveryReason,
  ): Promise<"recovered" | "session-expired" | "stopped"> {
    this.#callbacks.onState({
      kind: "recovering",
      cursor: this.#lastAppliedId,
      reason,
    });
    let attempt = 0;

    while (!this.#stopped) {
      const controller = new AbortController();
      this.#activeController = controller;
      try {
        const cursor = await this.#callbacks.recover(reason, controller.signal);
        if (this.#stopped) {
          return "stopped";
        }
        assertCursor(cursor);
        this.#lastAppliedId = cursor;
        return "recovered";
      } catch (error) {
        if (this.#stopped) {
          return "stopped";
        }
        if (isSessionExpiredError(error)) {
          return "session-expired";
        }
        const delayMs = computeBackoffDelay(
          attempt,
          this.#baseDelayMs,
          this.#maxDelayMs,
          this.#jitter,
        );
        attempt += 1;
        this.#callbacks.onState({
          kind: "unavailable",
          attempt,
          delayMs,
          reason,
          error,
        });
        if (!(await this.#wait(delayMs))) {
          return "stopped";
        }
      } finally {
        controller.abort();
        if (this.#activeController === controller) {
          this.#activeController = undefined;
        }
      }
    }
    return "stopped";
  }

  async #wait(delayMs: number): Promise<boolean> {
    if (this.#stopped) {
      return false;
    }
    const controller = new AbortController();
    this.#activeController = controller;
    try {
      await this.#sleep(delayMs, controller.signal);
      return !this.#stopped;
    } catch {
      return false;
    } finally {
      controller.abort();
      if (this.#activeController === controller) {
        this.#activeController = undefined;
      }
    }
  }
}

type DecodedFrame =
  | {
      readonly kind: "message";
      readonly message: SseMessage;
      readonly persistedId: number | null;
    }
  | {
      readonly kind: "diagnostic";
      readonly diagnostic: SseDiagnostic;
      readonly persistedId: number;
    }
  | { readonly kind: "reset"; readonly latestEventId: number; readonly persistedId: null };

function decodeFrame(frame: SseFrame, schemaVersion: number): DecodedFrame {
  let value: unknown;
  try {
    value = JSON.parse(frame.data) as unknown;
  } catch (error) {
    throw new SseProtocolError(
      "MALFORMED_JSON",
      "SSE data was not valid JSON",
      { cause: error },
    );
  }
  if (!isRecord(value)) {
    throw new SseProtocolError(
      "MALFORMED_ENVELOPE",
      "SSE data must be a JSON object",
    );
  }

  const bodySchema = value.schema_version;
  if (!Number.isSafeInteger(bodySchema) || Number(bodySchema) <= 0) {
    throw new SseProtocolError(
      "MALFORMED_ENVELOPE",
      "SSE envelope schema_version must be a positive integer",
    );
  }
  if (bodySchema !== schemaVersion) {
    throw new SseProtocolError(
      "UNSUPPORTED_SCHEMA",
      `unsupported SSE schema version ${String(bodySchema)}`,
    );
  }
  if (typeof value.kind !== "string" || value.kind.length === 0) {
    throw new SseProtocolError(
      "MALFORMED_ENVELOPE",
      "SSE envelope kind must be a non-empty string",
    );
  }
  if (frame.event !== value.kind) {
    throw new SseProtocolError(
      "EVENT_KIND_MISMATCH",
      `SSE event ${frame.event} disagreed with data kind ${value.kind}`,
    );
  }

  if (value.kind === "stream.reset") {
    if (
      frame.id !== undefined ||
      !isNonNegativeSafeInteger(value.latest_event_id)
    ) {
      throw new SseProtocolError(
        "MALFORMED_ENVELOPE",
        "stream.reset must be id-less and carry a non-negative latest_event_id",
      );
    }
    return {
      kind: "reset",
      latestEventId: value.latest_event_id,
      persistedId: null,
    };
  }

  if (value.kind === "service.state") {
    if (
      frame.id !== undefined ||
      !isNonNegativeSafeInteger(value.generation) ||
      !isServiceState(value.state)
    ) {
      throw new SseProtocolError(
        "MALFORMED_ENVELOPE",
        "service.state envelope was malformed",
      );
    }
    if (!isServiceStateMessage(value)) {
      throw new SseProtocolError(
        "MALFORMED_ENVELOPE",
        "service.state envelope was malformed",
      );
    }
    return { kind: "message", message: value, persistedId: null };
  }

  const frameId = parsePersistedId(frame.id);
  if (!isPositiveSafeInteger(value.id)) {
    throw new SseProtocolError(
      "MALFORMED_ENVELOPE",
      "persisted SSE data id must be a positive integer",
    );
  }
  if (frameId !== value.id) {
    throw new SseProtocolError(
      "EVENT_ID_MISMATCH",
      `SSE id ${frameId} disagreed with data id ${value.id}`,
    );
  }

  if (!TASK_EVENT_KINDS.has(value.kind as never)) {
    return {
      kind: "diagnostic",
      persistedId: frameId,
      diagnostic: {
        code: "UNKNOWN_EVENT_KIND",
        event: value.kind,
        persistedId: frameId,
        message: `ignored supported-schema event kind ${value.kind}`,
      },
    };
  }

  if (
    typeof value.task_id !== "string" ||
    value.task_id.length === 0 ||
    typeof value.created_at !== "string" ||
    !isKnownTaskEvent(value)
  ) {
    throw new SseProtocolError(
      "MALFORMED_ENVELOPE",
      `persisted ${value.kind} envelope was malformed`,
    );
  }
  return {
    kind: "message",
    message: value,
    persistedId: frameId,
  };
}

function isServiceStateMessage(
  value: unknown,
): value is Extract<SseMessage, { kind: "service.state" }> {
  return (
    isRecord(value) &&
    value.kind === "service.state" &&
    isPositiveSafeInteger(value.schema_version) &&
    isNonNegativeSafeInteger(value.generation) &&
    isServiceState(value.state)
  );
}

function isKnownTaskEvent(value: unknown): value is TaskEvent {
  if (
    !isRecord(value) ||
    !isPositiveSafeInteger(value.id) ||
    !isPositiveSafeInteger(value.schema_version) ||
    typeof value.task_id !== "string" ||
    typeof value.created_at !== "string" ||
    !isRecord(value.payload)
  ) {
    return false;
  }

  switch (value.kind) {
    case "task.queued":
      return isConsistentLifecycleTask(value, "queued");
    case "task.started":
      return isConsistentLifecycleTask(value, "running");
    case "task.completed":
      return isConsistentLifecycleTask(value, "completed");
    case "task.failed":
      return isConsistentLifecycleTask(value, "failed");
    case "task.cancelled":
      return isConsistentLifecycleTask(value, "cancelled");
    case "task.interrupted":
      return isConsistentLifecycleTask(value, "interrupted");
    case "plan.updated":
      return isPlanSnapshot(value.payload.plan);
    case "activity.appended":
      return isActivityEntry(value.payload.entry);
    case "diff.updated":
      return isDiffSnapshot(value.payload.diff);
    case "test.updated":
      return isTestSnapshot(value.payload.tests);
    default:
      return false;
  }
}

function isConsistentLifecycleTask(
  event: Record<string, unknown>,
  expectedStatus: Task["status"],
): boolean {
  if (!isRecord(event.payload) || !isTask(event.payload.task)) {
    return false;
  }
  return (
    event.payload.task.id === event.task_id &&
    event.payload.task.status === expectedStatus &&
    event.payload.task.last_event_id === event.id
  );
}

function isTask(value: unknown): value is Task {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.id === "string" &&
    typeof value.repository_id === "string" &&
    typeof value.client_request_id === "string" &&
    typeof value.prompt === "string" &&
    isTaskStatus(value.status) &&
    isNonNegativeSafeInteger(value.attempt) &&
    isNonNegativeSafeInteger(value.last_event_id) &&
    typeof value.created_at === "string" &&
    isOptionalNullableString(value.started_at) &&
    isOptionalNullableString(value.finished_at) &&
    isOptionalNullableString(value.retry_of) &&
    (value.failure === undefined ||
      value.failure === null ||
      isTaskFailure(value.failure))
  );
}

function isTaskFailure(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.code === "string" &&
    typeof value.message === "string" &&
    typeof value.retryable === "boolean"
  );
}

function isPlanSnapshot(value: unknown): boolean {
  return (
    isRecord(value) &&
    isNonNegativeSafeInteger(value.revision) &&
    Array.isArray(value.items) &&
    value.items.every(
      (item) =>
        isRecord(item) &&
        typeof item.id === "string" &&
        typeof item.title === "string" &&
        (item.status === "pending" ||
          item.status === "running" ||
          item.status === "completed"),
    )
  );
}

function isActivityEntry(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    typeof value.created_at === "string" &&
    typeof value.message === "string" &&
    (value.level === "info" || value.level === "warning" || value.level === "error")
  );
}

function isDiffSnapshot(value: unknown): boolean {
  return (
    isRecord(value) &&
    isNonNegativeSafeInteger(value.revision) &&
    Array.isArray(value.files) &&
    value.files.every(
      (file) =>
        isRecord(file) &&
        typeof file.path === "string" &&
        typeof file.patch === "string" &&
        isNonNegativeSafeInteger(file.additions) &&
        isNonNegativeSafeInteger(file.deletions) &&
        (file.status === "added" ||
          file.status === "modified" ||
          file.status === "deleted"),
    )
  );
}

function isTestSnapshot(value: unknown): boolean {
  return (
    isRecord(value) &&
    isNonNegativeSafeInteger(value.revision) &&
    isTestStatus(value.status) &&
    Array.isArray(value.cases) &&
    value.cases.every(
      (testCase) =>
        isRecord(testCase) &&
        typeof testCase.id === "string" &&
        typeof testCase.name === "string" &&
        typeof testCase.summary === "string" &&
        isNonNegativeSafeInteger(testCase.duration_ms) &&
        isTestStatus(testCase.status),
    )
  );
}

function isTaskStatus(value: unknown): boolean {
  return (
    value === "queued" ||
    value === "running" ||
    value === "completed" ||
    value === "failed" ||
    value === "cancelled" ||
    value === "interrupted"
  );
}

function isTestStatus(value: unknown): boolean {
  return (
    value === "queued" ||
    value === "running" ||
    value === "passed" ||
    value === "failed" ||
    value === "cancelled"
  );
}

function isOptionalNullableString(value: unknown): boolean {
  return value === undefined || value === null || typeof value === "string";
}

function parsePersistedId(value: string | undefined): number {
  if (value === undefined || !/^[1-9]\d*$/.test(value)) {
    throw new SseProtocolError(
      "MALFORMED_ENVELOPE",
      "persisted SSE frame id must be a positive integer",
    );
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new SseProtocolError(
      "MALFORMED_ENVELOPE",
      "persisted SSE frame id exceeded the safe integer range",
    );
  }
  return parsed;
}

function protocolExit(
  error: unknown,
  cursorAdvanced: boolean,
): ConnectionExit {
  if (error instanceof SseProtocolError) {
    return {
      kind: "protocol",
      reason: { code: error.code, message: error.message },
      cursorAdvanced,
    };
  }
  return {
    kind: "protocol",
    reason: {
      code: "PROJECTION_ERROR",
      message: "unexpected SSE projection failure",
    },
    cursorAdvanced,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPositiveSafeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && Number(value) > 0;
}

function isNonNegativeSafeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && Number(value) >= 0;
}

function isServiceState(value: unknown): value is "ready" | "store_degraded" | "quiescing" {
  return value === "ready" || value === "store_degraded" || value === "quiescing";
}

function isEventStreamContentType(contentType: string | null): boolean {
  return contentType
    ?.split(";", 1)[0]
    ?.trim()
    .toLowerCase() === "text/event-stream";
}

function isTransientStatus(status: number): boolean {
  return status === 408 || status === 429 || (status >= 500 && status <= 599);
}

async function cancelResponseBody(response: Response): Promise<void> {
  if (response.body === null) {
    return;
  }
  try {
    await response.body.cancel();
  } catch {
    // Response cleanup must not overwrite its recovery classification.
  }
}

function isSessionExpiredError(error: unknown): boolean {
  return (
    isRecord(error) &&
    (error.status === 401 || error.name === "SessionExpiredError")
  );
}

function withCursor(endpoint: string, cursor: number): string {
  const hashIndex = endpoint.indexOf("#");
  const withoutHash = hashIndex < 0 ? endpoint : endpoint.slice(0, hashIndex);
  const queryIndex = withoutHash.indexOf("?");
  const path = queryIndex < 0 ? withoutHash : withoutHash.slice(0, queryIndex);
  const params = new URLSearchParams(
    queryIndex < 0 ? "" : withoutHash.slice(queryIndex + 1),
  );
  params.set("after", String(cursor));
  return `${path}?${params.toString()}`;
}

function positiveDelay(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function assertCursor(cursor: number): void {
  if (!isNonNegativeSafeInteger(cursor)) {
    throw new RangeError("SSE cursor must be a non-negative safe integer");
  }
}

export function computeBackoffDelay(
  attempt: number,
  baseDelayMs: number,
  maxDelayMs: number,
  jitter: () => number = Math.random,
): number {
  if (!Number.isSafeInteger(attempt) || attempt < 0) {
    throw new RangeError("backoff attempt must be a non-negative safe integer");
  }
  positiveDelay(baseDelayMs, "baseDelayMs");
  positiveDelay(maxDelayMs, "maxDelayMs");
  const exponent = Math.min(attempt, 52);
  const capped = Math.min(maxDelayMs, baseDelayMs * 2 ** exponent);
  const sample = Math.min(1, Math.max(0, jitter()));
  return Math.max(1, Math.floor(capped * (0.5 + sample * 0.5)));
}

function abortableSleep(delayMs: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new DOMException("sleep aborted", "AbortError"));
      return;
    }
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, delayMs);
    const abort = () => {
      clearTimeout(timer);
      reject(new DOMException("sleep aborted", "AbortError"));
    };
    signal.addEventListener("abort", abort, { once: true });
  });
}
