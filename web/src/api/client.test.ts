import { describe, expect, it, vi } from "vitest";

import {
  ApiClient,
  ApiError,
  SessionExpiredError,
  type ApiClientOptions,
} from "./client";
import type { BootstrapResponse, Task } from "./types";

const BOOTSTRAP: BootstrapResponse = {
  csrf_token: "csrf-from-bootstrap",
  latest_event_id: 17,
  max_concurrent_tasks: 2,
  repositories: [],
  server_started_at: "2026-07-15T00:00:00Z",
  service_state: "ready",
  service_state_generation: 4,
  tasks: [],
};

const TASK: Task = {
  attempt: 1,
  client_request_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  created_at: "2026-07-15T00:00:00Z",
  failure: null,
  finished_at: null,
  id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  last_event_id: 17,
  prompt: "fix it",
  repository_id: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
  retry_of: null,
  started_at: null,
  status: "queued",
};

function jsonResponse(body: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
    ...init,
  });
}

function clientOptions(
  fetch: typeof globalThis.fetch,
  overrides: Partial<ApiClientOptions> = {},
): ApiClientOptions {
  return {
    fetch,
    location: {
      hash: "",
      pathname: "/workspace",
      search: "?view=tasks",
    },
    history: { replaceState: vi.fn() },
    randomUUID: () => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    ...overrides,
  };
}

describe("ApiClient session initialization", () => {
  it("removes the launch-token fragment synchronously before exchange fetch", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    let fragmentWasRemoved = false;
    const history = {
      replaceState: vi.fn((_data: unknown, _unused: string, url?: string | URL | null) => {
        expect(url).toBe("/workspace?view=tasks");
        fragmentWasRemoved = true;
      }),
    };
    const fetch = vi.fn<typeof globalThis.fetch>(async (input, init) => {
      expect(fragmentWasRemoved).toBe(true);
      calls.push({ input, ...(init === undefined ? {} : { init }) });
      return calls.length === 1
        ? new Response(null, { status: 204 })
        : jsonResponse(BOOTSTRAP);
    });
    const client = new ApiClient(
      clientOptions(fetch, {
        history,
        location: {
          hash: "#token=launch%20secret",
          pathname: "/workspace",
          search: "?view=tasks",
        },
      }),
    );

    expect(history.replaceState).toHaveBeenCalledOnce();
    expect(fetch).not.toHaveBeenCalled();

    await expect(client.initialize()).resolves.toEqual(BOOTSTRAP);
    expect(calls.map(({ input }) => input)).toEqual([
      "/api/session/exchange",
      "/api/bootstrap",
    ]);
    expect(calls[0]?.init).toMatchObject({
      method: "POST",
      credentials: "same-origin",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token: "launch secret" }),
    });
  });

  it("uses the existing host-only cookie when there is no fragment", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => jsonResponse(BOOTSTRAP));
    const history = { replaceState: vi.fn() };
    const client = new ApiClient(clientOptions(fetch, { history }));

    await expect(client.initialize()).resolves.toEqual(BOOTSTRAP);

    expect(history.replaceState).not.toHaveBeenCalled();
    expect(fetch).toHaveBeenCalledOnce();
    expect(fetch).toHaveBeenCalledWith(
      "/api/bootstrap",
      expect.objectContaining({ credentials: "same-origin", method: "GET" }),
    );
    expect(client.csrfToken).toBe("csrf-from-bootstrap");
  });

  it("shares one in-flight initialization and never bootstraps before exchange completes", async () => {
    let releaseExchange: ((response: Response) => void) | undefined;
    let exchangeCompleted = false;
    let bootstrapStartedEarly = false;
    const paths: Array<RequestInfo | URL> = [];
    const fetch = vi.fn<typeof globalThis.fetch>(async (input) => {
      paths.push(input);
      if (input === "/api/session/exchange") {
        return new Promise<Response>((resolve) => {
          releaseExchange = resolve;
        });
      }
      bootstrapStartedEarly ||= !exchangeCompleted;
      return jsonResponse(BOOTSTRAP);
    });
    const client = new ApiClient(
      clientOptions(fetch, {
        location: {
          hash: "#token=one-use-token",
          pathname: "/workspace",
          search: "",
        },
      }),
    );

    const first = client.initialize();
    const second = client.initialize();
    await vi.waitFor(() => expect(releaseExchange).toBeTypeOf("function"));
    exchangeCompleted = true;
    releaseExchange?.(new Response(null, { status: 204 }));

    await expect(Promise.all([first, second])).resolves.toEqual([BOOTSTRAP, BOOTSTRAP]);
    expect(first).toBe(second);
    expect(bootstrapStartedEarly).toBe(false);
    expect(paths).toEqual(["/api/session/exchange", "/api/bootstrap"]);
  });

  it("allows a non-session initialization failure to retry safely without replaying the token", async () => {
    const networkFailure = new TypeError("exchange outcome is unknown");
    const paths: Array<RequestInfo | URL> = [];
    const fetch = vi.fn<typeof globalThis.fetch>(async (input) => {
      paths.push(input);
      if (paths.length === 1) throw networkFailure;
      return jsonResponse(BOOTSTRAP);
    });
    const client = new ApiClient(
      clientOptions(fetch, {
        location: {
          hash: "#token=one-use-token",
          pathname: "/workspace",
          search: "",
        },
      }),
    );

    await expect(client.initialize()).rejects.toMatchObject({
      code: "NETWORK_ERROR",
      retryable: true,
      requestId: null,
      details: {},
      cause: networkFailure,
    });
    await expect(client.initialize()).resolves.toEqual(BOOTSTRAP);
    expect(paths).toEqual(["/api/session/exchange", "/api/bootstrap"]);
  });

  it("treats a 401 as an expired session and notifies its owner", async () => {
    const onSessionExpired = vi.fn();
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse(
        {
          code: "SESSION_EXPIRED",
          message: "reopen from the native application",
          retryable: false,
          request_id: "request-401",
          details: {},
        },
        { status: 401 },
      ),
    );
    const client = new ApiClient(clientOptions(fetch, { onSessionExpired }));

    const error = await client.initialize().catch((reason: unknown) => reason);

    expect(error).toBeInstanceOf(SessionExpiredError);
    expect(error).toMatchObject({
      status: 401,
      code: "SESSION_EXPIRED",
      requestId: "request-401",
      retryable: false,
    });
    expect(onSessionExpired).toHaveBeenCalledOnce();
    expect(fetch).toHaveBeenCalledOnce();

    await expect(client.initialize()).rejects.toBe(error);
    expect(fetch).toHaveBeenCalledOnce();
  });
});

describe("ApiClient REST commands", () => {
  it("forwards an AbortSignal to recovery bootstrap fetch", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => jsonResponse(BOOTSTRAP));
    const client = new ApiClient(clientOptions(fetch));
    await client.initialize();
    const controller = new AbortController();

    await client.bootstrap(controller.signal);

    expect(fetch).toHaveBeenLastCalledWith(
      "/api/bootstrap",
      expect.objectContaining({ signal: controller.signal }),
    );
  });

  it("uses relative same-origin JSON requests and bootstrap CSRF for mutations", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const fetch = vi.fn<typeof globalThis.fetch>(async (input, init) => {
      calls.push({ input, ...(init === undefined ? {} : { init }) });
      if (calls.length === 1) return jsonResponse(BOOTSTRAP);
      if (input === "/api/repositories/pick") return new Response(null, { status: 204 });
      return jsonResponse(TASK);
    });
    const client = new ApiClient(clientOptions(fetch));
    await client.initialize();

    await client.taskDetail("task/id");
    await expect(client.cancelTask("task/id")).resolves.toEqual({
      task: TASK,
      cancellation_requested: false,
    });
    await expect(client.pickRepository()).resolves.toBeNull();

    expect(calls[1]).toMatchObject({
      input: "/api/tasks/task%2Fid",
      init: { method: "GET", credentials: "same-origin" },
    });
    expect(calls[2]).toMatchObject({
      input: "/api/tasks/task%2Fid/cancel",
      init: {
        method: "POST",
        credentials: "same-origin",
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          "x-csrf-token": "csrf-from-bootstrap",
        },
      },
    });
    expect(calls[3]).toMatchObject({
      input: "/api/repositories/pick",
      init: {
        method: "POST",
        credentials: "same-origin",
        headers: expect.objectContaining({
          "x-csrf-token": "csrf-from-bootstrap",
        }),
      },
    });
    expect(calls.every(({ input }) => typeof input === "string" && input.startsWith("/"))).toBe(
      true,
    );
  });

  it("surfaces the structured API error without automatically retrying a mutation", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async (_input, _init) => {
      if (fetch.mock.calls.length === 1) return jsonResponse(BOOTSTRAP);
      return jsonResponse(
        {
          code: "STORE_BUSY",
          message: "database writer is busy",
          retryable: true,
          request_id: "request-503",
          details: { retry_after_ms: 250 },
        },
        { status: 503 },
      );
    });
    const client = new ApiClient(clientOptions(fetch));
    await client.initialize();

    const error = await client.cancelTask(TASK.id).catch((reason: unknown) => reason);

    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({
      status: 503,
      code: "STORE_BUSY",
      message: "database writer is busy",
      retryable: true,
      requestId: "request-503",
      details: { retry_after_ms: 250 },
    });
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("reuses one client request UUID when the caller explicitly retries an ambiguous create", async () => {
    const bodies: string[] = [];
    const networkFailure = new TypeError("connection closed after send");
    const fetch = vi.fn<typeof globalThis.fetch>(async (_input, init) => {
      if (fetch.mock.calls.length === 1) return jsonResponse(BOOTSTRAP);
      bodies.push(String(init?.body));
      if (bodies.length === 1) throw networkFailure;
      return jsonResponse(TASK, { status: 201 });
    });
    const randomUUID = vi.fn(() => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
    const client = new ApiClient(clientOptions(fetch, { randomUUID }));
    await client.initialize();
    const create = client.newCreateTask(TASK.repository_id, "fix it");

    await expect(create.execute()).rejects.toMatchObject({
      status: 0,
      code: "NETWORK_ERROR",
      message: "the network request failed",
      retryable: true,
      requestId: null,
      details: {},
      cause: networkFailure,
    });
    await expect(create.execute()).resolves.toEqual(TASK);

    expect(randomUUID).toHaveBeenCalledOnce();
    expect(create.clientRequestId).toBe("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
    expect(bodies).toHaveLength(2);
    expect(bodies.map((body) => JSON.parse(body))).toEqual([
      {
        client_request_id: create.clientRequestId,
        repository_id: TASK.repository_id,
        prompt: "fix it",
      },
      {
        client_request_id: create.clientRequestId,
        repository_id: TASK.repository_id,
        prompt: "fix it",
      },
    ]);
  });

  it("retains the create UUID when STORE_BUSY invites an explicit UI retry", async () => {
    const bodies: string[] = [];
    const fetch = vi.fn<typeof globalThis.fetch>(async (_input, init) => {
      if (fetch.mock.calls.length === 1) return jsonResponse(BOOTSTRAP);
      bodies.push(String(init?.body));
      if (bodies.length === 1) {
        return jsonResponse(
          {
            code: "STORE_BUSY",
            message: "try again",
            retryable: true,
            request_id: "busy-create",
            details: {},
          },
          { status: 503 },
        );
      }
      return jsonResponse(TASK);
    });
    const client = new ApiClient(clientOptions(fetch));
    await client.initialize();
    const create = client.newCreateTask(TASK.repository_id, TASK.prompt);

    await expect(create.execute()).rejects.toMatchObject({
      code: "STORE_BUSY",
      retryable: true,
      requestId: "busy-create",
    });
    await expect(create.execute()).resolves.toEqual(TASK);

    expect(JSON.parse(bodies[0] ?? "null").client_request_id).toBe(create.clientRequestId);
    expect(bodies[1]).toBe(bodies[0]);
  });
});
