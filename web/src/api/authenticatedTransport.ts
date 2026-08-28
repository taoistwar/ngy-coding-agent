import type { ApiErrorResponse } from "./types";
import { ValidationError } from "./validation";

export interface AuthenticatedTransportOptions {
  fetch?: typeof globalThis.fetch;
  onSessionExpired?: (error: SessionExpiredError) => void;
}

export interface AuthenticatedRequestOptions {
  method?: "GET" | "POST";
  body?: unknown;
  mutation?: boolean;
  signal?: AbortSignal;
}

export type ResponseValidator<T> = (value: unknown) => T;

const API_JSON_HEADERS = {
  accept: "application/json",
  "content-type": "application/json",
} as const;

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly retryable: boolean;
  readonly requestId: string | null;
  readonly details: Record<string, unknown>;

  constructor(
    status: number,
    code: string,
    message: string,
    retryable: boolean,
    requestId: string | null,
    details: Record<string, unknown>,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.retryable = retryable;
    this.requestId = requestId;
    this.details = details;
  }
}

export class SessionExpiredError extends ApiError {
  constructor(
    status: number,
    code: string,
    message: string,
    retryable: boolean,
    requestId: string | null,
    details: Record<string, unknown>,
  ) {
    super(status, code, message, retryable, requestId, details);
    this.name = "SessionExpiredError";
  }
}

export class AuthenticatedTransport {
  readonly #fetch: typeof globalThis.fetch;
  #onSessionExpired: ((error: SessionExpiredError) => void) | undefined;
  #lastNotifiedSessionHandler:
    | ((error: SessionExpiredError) => void)
    | undefined;
  #csrfToken: string | null = null;
  #sessionExpired: SessionExpiredError | null = null;

  constructor(options: AuthenticatedTransportOptions = {}) {
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.#onSessionExpired = options.onSessionExpired;
  }

  get csrfToken(): string | null {
    return this.#csrfToken;
  }

  setCsrfToken(csrfToken: string): void {
    this.#csrfToken = csrfToken;
  }

  setSessionExpiredHandler(
    handler: (error: SessionExpiredError) => void,
  ): () => void {
    this.#onSessionExpired = handler;
    this.#notifySessionExpired();
    return () => {
      if (this.#onSessionExpired === handler) {
        this.#onSessionExpired = undefined;
      }
    };
  }

  async request<T>(
    path: string,
    options: AuthenticatedRequestOptions = {},
    validate?: ResponseValidator<T>,
  ): Promise<T> {
    if (this.#sessionExpired !== null) {
      throw this.#sessionExpired;
    }

    const method = options.method ?? "GET";
    const headers: Record<string, string> = { ...API_JSON_HEADERS };
    if (options.mutation === true) {
      if (this.#csrfToken === null) {
        throw new ApiError(
          0,
          "SESSION_NOT_INITIALIZED",
          "bootstrap must complete before a mutation",
          false,
          null,
          {},
        );
      }
      headers["x-csrf-token"] = this.#csrfToken;
    }

    const init: RequestInit = {
      method,
      credentials: "same-origin",
      headers,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
      ...(options.body === undefined
        ? {}
        : { body: JSON.stringify(options.body) }),
    };
    let response: Response;
    try {
      response = await this.#fetch(path, init);
    } catch (cause) {
      throw new ApiError(
        0,
        "NETWORK_ERROR",
        "the network request failed",
        true,
        null,
        {},
        { cause },
      );
    }
    if (!response.ok) {
      throw await this.#responseError(response);
    }
    if (response.status === 204) {
      return null as T;
    }

    const value = await decodeJson(response);
    if (validate === undefined) {
      return value as T;
    }
    try {
      return validate(value);
    } catch (cause) {
      if (!(cause instanceof ValidationError)) {
        throw cause;
      }
      throw new ApiError(
        response.status,
        "INVALID_RESPONSE",
        "the server response violated the API contract",
        false,
        response.headers.get("x-request-id"),
        { path: cause.path },
        { cause },
      );
    }
  }

  async #responseError(response: Response): Promise<ApiError> {
    const payload = await readApiError(response);
    const requestId = payload?.request_id ?? response.headers.get("x-request-id");
    const code = payload?.code ?? `HTTP_${response.status}`;
    const message = payload?.message ?? `request failed with HTTP ${response.status}`;
    const retryable = payload?.retryable ?? false;
    const details = payload?.details ?? {};

    if (response.status === 401) {
      if (this.#sessionExpired !== null) {
        return this.#sessionExpired;
      }
      const expired = new SessionExpiredError(
        response.status,
        code,
        message,
        retryable,
        requestId,
        details,
      );
      this.#sessionExpired = expired;
      this.#csrfToken = null;
      this.#notifySessionExpired();
      return expired;
    }

    return new ApiError(response.status, code, message, retryable, requestId, details);
  }

  #notifySessionExpired(): void {
    const handler = this.#onSessionExpired;
    const expired = this.#sessionExpired;
    if (
      handler !== undefined &&
      expired !== null &&
      this.#lastNotifiedSessionHandler !== handler
    ) {
      this.#lastNotifiedSessionHandler = handler;
      handler(expired);
    }
  }
}

async function decodeJson(response: Response): Promise<unknown> {
  const text = await response.text();
  try {
    return JSON.parse(text) as unknown;
  } catch (cause) {
    throw new ApiError(
      response.status,
      "INVALID_RESPONSE",
      "the server returned invalid JSON",
      false,
      response.headers.get("x-request-id"),
      {},
      { cause },
    );
  }
}

async function readApiError(response: Response): Promise<ApiErrorResponse | null> {
  let candidate: unknown;
  try {
    candidate = await response.json();
  } catch {
    return null;
  }
  return isApiErrorResponse(candidate) ? candidate : null;
}

function isApiErrorResponse(value: unknown): value is ApiErrorResponse {
  if (!isRecord(value)) return false;
  return (
    typeof value.code === "string" &&
    typeof value.message === "string" &&
    typeof value.retryable === "boolean" &&
    typeof value.request_id === "string" &&
    isRecord(value.details)
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
