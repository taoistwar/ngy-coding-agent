import type {
  AddRepositoryRequest,
  ApiErrorResponse,
  BootstrapResponse,
  CancellationAcceptedResponse,
  CreateTaskRequest,
  Repository,
  QuitResponse,
  SessionExchangeRequest,
  Task,
  TaskDetail,
  TaskEvent,
} from "./types";

export interface LocationLike {
  readonly hash: string;
  readonly pathname: string;
  readonly search: string;
}

export interface HistoryLike {
  readonly state?: unknown;
  replaceState(data: unknown, unused: string, url?: string | URL | null): void;
}

export interface ApiClientOptions {
  fetch?: typeof globalThis.fetch;
  location?: LocationLike;
  history?: HistoryLike;
  randomUUID?: () => string;
  onSessionExpired?: (error: SessionExpiredError) => void;
}

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

export interface CreateTaskCommand {
  readonly clientRequestId: string;
  execute(): Promise<Task>;
}

interface RequestOptions {
  method?: "GET" | "POST";
  body?: unknown;
  mutation?: boolean;
  signal?: AbortSignal;
}

const API_JSON_HEADERS = {
  accept: "application/json",
  "content-type": "application/json",
} as const;

export class ApiClient {
  readonly #fetch: typeof globalThis.fetch;
  readonly #randomUUID: () => string;
  readonly #onSessionExpired: ((error: SessionExpiredError) => void) | undefined;
  #launchToken: string | null;
  #csrfToken: string | null = null;
  #sessionExpired: SessionExpiredError | null = null;
  #initialization: Promise<BootstrapResponse> | null = null;

  constructor(options: ApiClientOptions = {}) {
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.#randomUUID = options.randomUUID ?? (() => globalThis.crypto.randomUUID());
    this.#onSessionExpired = options.onSessionExpired;

    const location = options.location ?? globalThis.location;
    const history = options.history ?? globalThis.history;
    this.#launchToken = readLaunchToken(location.hash);

    if (location.hash.length > 0) {
      history.replaceState(history.state ?? null, "", `${location.pathname}${location.search}`);
    }
  }

  get csrfToken(): string | null {
    return this.#csrfToken;
  }

  initialize(): Promise<BootstrapResponse> {
    if (this.#initialization !== null) return this.#initialization;

    const initialization = this.#initializeOnce();
    this.#initialization = initialization;
    void initialization.catch((error: unknown) => {
      if (this.#initialization === initialization && !(error instanceof SessionExpiredError)) {
        this.#initialization = null;
      }
    });
    return initialization;
  }

  async #initializeOnce(): Promise<BootstrapResponse> {
    const launchToken = this.#launchToken;
    this.#launchToken = null;
    if (launchToken !== null) {
      const body: SessionExchangeRequest = { token: launchToken };
      await this.#request<void>("/api/session/exchange", {
        method: "POST",
        body,
      });
    }
    return this.bootstrap();
  }

  async bootstrap(signal?: AbortSignal): Promise<BootstrapResponse> {
    const bootstrap = await this.#request<BootstrapResponse>("/api/bootstrap", {
      ...(signal === undefined ? {} : { signal }),
    });
    this.#csrfToken = bootstrap.csrf_token;
    return bootstrap;
  }

  listRepositories(): Promise<Repository[]> {
    return this.#request<Repository[]>("/api/repositories");
  }

  addRepository(path: string): Promise<Repository> {
    const body: AddRepositoryRequest = { path };
    return this.#request<Repository>("/api/repositories", {
      method: "POST",
      mutation: true,
      body,
    });
  }

  async pickRepository(): Promise<Repository | null> {
    return this.#request<Repository | null>("/api/repositories/pick", {
      method: "POST",
      mutation: true,
    });
  }

  listTasks(repositoryId?: string): Promise<Task[]> {
    const query =
      repositoryId === undefined
        ? ""
        : `?${new URLSearchParams({ repository_id: repositoryId }).toString()}`;
    return this.#request<Task[]>(`/api/tasks${query}`);
  }

  createTask(request: CreateTaskRequest): Promise<Task> {
    return this.#request<Task>("/api/tasks", {
      method: "POST",
      mutation: true,
      body: request,
    });
  }

  newCreateTask(repositoryId: string, prompt: string): CreateTaskCommand {
    const clientRequestId = this.#randomUUID();
    const request: CreateTaskRequest = {
      client_request_id: clientRequestId,
      repository_id: repositoryId,
      prompt,
    };
    return {
      clientRequestId,
      execute: () => this.createTask(request),
    };
  }

  taskDetail(taskId: string): Promise<TaskDetail> {
    return this.#request<TaskDetail>(`/api/tasks/${encodeURIComponent(taskId)}`);
  }

  async cancelTask(taskId: string): Promise<CancellationAcceptedResponse> {
    const response = await this.#request<Task | CancellationAcceptedResponse>(
      `/api/tasks/${encodeURIComponent(taskId)}/cancel`,
      { method: "POST", mutation: true },
    );
    return isCancellationAccepted(response)
      ? response
      : { task: response, cancellation_requested: false };
  }

  retryTask(taskId: string): Promise<Task> {
    return this.#request<Task>(`/api/tasks/${encodeURIComponent(taskId)}/retry`, {
      method: "POST",
      mutation: true,
    });
  }

  taskEvents(taskId: string, after?: number): Promise<TaskEvent[]> {
    const query = after === undefined ? "" : `?${new URLSearchParams({ after: String(after) })}`;
    return this.#request<TaskEvent[]>(
      `/api/tasks/${encodeURIComponent(taskId)}/events${query}`,
    );
  }

  quit(): Promise<QuitResponse> {
    return this.#request<QuitResponse>("/api/app/quit", {
      method: "POST",
      mutation: true,
    });
  }

  async #request<T>(path: string, options: RequestOptions = {}): Promise<T> {
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
      ...(options.body === undefined ? {} : { body: JSON.stringify(options.body) }),
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

    return (await decodeJson(response)) as T;
  }

  async #responseError(response: Response): Promise<ApiError> {
    const payload = await readApiError(response);
    const requestId = payload?.request_id ?? response.headers.get("x-request-id");
    const code = payload?.code ?? `HTTP_${response.status}`;
    const message = payload?.message ?? `request failed with HTTP ${response.status}`;
    const retryable = payload?.retryable ?? false;
    const details = payload?.details ?? {};

    if (response.status === 401) {
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
      this.#onSessionExpired?.(expired);
      return expired;
    }

    return new ApiError(response.status, code, message, retryable, requestId, details);
  }
}

function readLaunchToken(hash: string): string | null {
  if (hash.length === 0) return null;
  return new URLSearchParams(hash.startsWith("#") ? hash.slice(1) : hash).get("token");
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

function isCancellationAccepted(
  value: Task | CancellationAcceptedResponse,
): value is CancellationAcceptedResponse {
  return isRecord(value) && "cancellation_requested" in value && "task" in value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
