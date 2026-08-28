import type {
  AddRepositoryRequest,
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
import {
  validateBootstrapResponse,
  validateCancellationResponse,
  validateQuitResponse,
  validateRepository,
  validateRepositoryList,
  validateTask,
  validateTaskDetail,
  validateTaskEventList,
  validateTaskList,
} from "./validation";
import {
  AuthenticatedTransport,
  SessionExpiredError,
} from "./authenticatedTransport";

export { ApiError, SessionExpiredError } from "./authenticatedTransport";

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
  transport?: AuthenticatedTransport;
  fetch?: typeof globalThis.fetch;
  location?: LocationLike;
  history?: HistoryLike;
  randomUUID?: () => string;
  onSessionExpired?: (error: SessionExpiredError) => void;
}

export interface CreateTaskCommand {
  readonly clientRequestId: string;
  execute(): Promise<Task>;
}

export class ApiClient {
  readonly #transport: AuthenticatedTransport;
  readonly #randomUUID: () => string;
  #launchToken: string | null;
  #initialization: Promise<BootstrapResponse> | null = null;

  constructor(options: ApiClientOptions = {}) {
    this.#transport =
      options.transport ??
      new AuthenticatedTransport({
        ...(options.fetch === undefined ? {} : { fetch: options.fetch }),
        ...(options.onSessionExpired === undefined
          ? {}
          : { onSessionExpired: options.onSessionExpired }),
      });
    this.#randomUUID = options.randomUUID ?? (() => globalThis.crypto.randomUUID());

    const location = options.location ?? globalThis.location;
    const history = options.history ?? globalThis.history;
    this.#launchToken = readLaunchToken(location.hash);

    if (location.hash.length > 0) {
      history.replaceState(history.state ?? null, "", `${location.pathname}${location.search}`);
    }
  }

  get csrfToken(): string | null {
    return this.#transport.csrfToken;
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
      await this.#transport.request<void>("/api/session/exchange", {
        method: "POST",
        body,
      });
    }
    return this.bootstrap();
  }

  async bootstrap(signal?: AbortSignal): Promise<BootstrapResponse> {
    const bootstrap = await this.#transport.request<BootstrapResponse>(
      "/api/bootstrap",
      {
        ...(signal === undefined ? {} : { signal }),
      },
      validateBootstrapResponse,
    );
    this.#transport.setCsrfToken(bootstrap.csrf_token);
    return bootstrap;
  }

  listRepositories(): Promise<Repository[]> {
    return this.#transport.request<Repository[]>(
      "/api/repositories",
      {},
      validateRepositoryList,
    );
  }

  addRepository(path: string): Promise<Repository> {
    const body: AddRepositoryRequest = { path };
    return this.#transport.request<Repository>(
      "/api/repositories",
      {
        method: "POST",
        mutation: true,
        body,
      },
      validateRepository,
    );
  }

  async pickRepository(): Promise<Repository | null> {
    return this.#transport.request<Repository | null>(
      "/api/repositories/pick",
      {
        method: "POST",
        mutation: true,
      },
      (value) => (value === null ? null : validateRepository(value)),
    );
  }

  listTasks(repositoryId?: string): Promise<Task[]> {
    const query =
      repositoryId === undefined
        ? ""
        : `?${new URLSearchParams({ repository_id: repositoryId }).toString()}`;
    return this.#transport.request<Task[]>(
      `/api/tasks${query}`,
      {},
      validateTaskList,
    );
  }

  createTask(request: CreateTaskRequest): Promise<Task> {
    return this.#transport.request<Task>(
      "/api/tasks",
      {
        method: "POST",
        mutation: true,
        body: request,
      },
      validateTask,
    );
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
    return this.#transport.request<TaskDetail>(
      `/api/tasks/${encodeURIComponent(taskId)}`,
      {},
      validateTaskDetail,
    );
  }

  async cancelTask(taskId: string): Promise<CancellationAcceptedResponse> {
    const response = await this.#transport.request<Task | CancellationAcceptedResponse>(
      `/api/tasks/${encodeURIComponent(taskId)}/cancel`,
      { method: "POST", mutation: true },
      validateCancellationResponse,
    );
    return isCancellationAccepted(response)
      ? response
      : { task: response, cancellation_requested: false };
  }

  retryTask(taskId: string): Promise<Task> {
    return this.#transport.request<Task>(
      `/api/tasks/${encodeURIComponent(taskId)}/retry`,
      {
        method: "POST",
        mutation: true,
      },
      validateTask,
    );
  }

  taskEvents(taskId: string, after?: number): Promise<TaskEvent[]> {
    const query = after === undefined ? "" : `?${new URLSearchParams({ after: String(after) })}`;
    return this.#transport.request<TaskEvent[]>(
      `/api/tasks/${encodeURIComponent(taskId)}/events${query}`,
      {},
      validateTaskEventList,
    );
  }

  quit(): Promise<QuitResponse> {
    return this.#transport.request<QuitResponse>(
      "/api/app/quit",
      {
        method: "POST",
        mutation: true,
      },
      validateQuitResponse,
    );
  }

}

function readLaunchToken(hash: string): string | null {
  if (hash.length === 0) return null;
  return new URLSearchParams(hash.startsWith("#") ? hash.slice(1) : hash).get("token");
}

function isCancellationAccepted(
  value: Task | CancellationAcceptedResponse,
): value is CancellationAcceptedResponse {
  return isRecord(value) && "cancellation_requested" in value && "task" in value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
