import { describe, expect, it, vi } from "vitest";

import {
  ApiError,
  AuthenticatedTransport,
  SessionExpiredError,
} from "./authenticatedTransport";
import { ValidationError } from "./validation";

function jsonResponse(body: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
    ...init,
  });
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

describe("AuthenticatedTransport", () => {
  it("owns same-origin JSON, CSRF, body, and AbortSignal semantics", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => jsonResponse({ ok: true }));
    const transport = new AuthenticatedTransport({ fetch });
    const controller = new AbortController();

    await expect(
      transport.request("/api/mutation", { method: "POST", mutation: true }),
    ).rejects.toMatchObject({ code: "SESSION_NOT_INITIALIZED" });

    transport.setCsrfToken("csrf-token");
    await expect(
      transport.request<{ ok: boolean }>(
        "/api/mutation",
        {
          method: "POST",
          mutation: true,
          body: { exact: "body" },
          signal: controller.signal,
        },
        (value) => value as { ok: boolean },
      ),
    ).resolves.toEqual({ ok: true });

    expect(fetch).toHaveBeenCalledOnce();
    expect(fetch).toHaveBeenCalledWith(
      "/api/mutation",
      expect.objectContaining({
        method: "POST",
        credentials: "same-origin",
        signal: controller.signal,
        body: JSON.stringify({ exact: "body" }),
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          "x-csrf-token": "csrf-token",
        },
      }),
    );
  });

  it("normalizes network and exact-response validation failures once", async () => {
    const networkFailure = new DOMException("aborted", "AbortError");
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockRejectedValueOnce(networkFailure)
      .mockResolvedValueOnce(
        jsonResponse(
          { unexpected: true },
          { headers: { "x-request-id": "invalid-response-request" } },
        ),
      );
    const transport = new AuthenticatedTransport({ fetch });

    const first = await transport.request("/api/first").catch((error: unknown) => error);
    expect(first).toBeInstanceOf(ApiError);
    expect(first).toMatchObject({
      status: 0,
      code: "NETWORK_ERROR",
      retryable: true,
      cause: networkFailure,
    });

    await expect(
      transport.request("/api/second", {}, () => {
        throw new ValidationError("$.expected", "missing required field");
      }),
    ).rejects.toMatchObject({
      status: 200,
      code: "INVALID_RESPONSE",
      requestId: "invalid-response-request",
      details: { path: "$.expected" },
    });
  });

  it("latches one 401, clears CSRF, and reports its request ID", async () => {
    const onSessionExpired = vi.fn();
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse(
        {
          code: "SESSION_EXPIRED",
          message: "reopen",
          retryable: false,
          request_id: "request-401",
          details: {},
        },
        { status: 401 },
      ),
    );
    const transport = new AuthenticatedTransport({ fetch, onSessionExpired });
    transport.setCsrfToken("csrf-token");

    const first = await transport.request("/api/protected").catch((error: unknown) => error);
    expect(first).toBeInstanceOf(SessionExpiredError);
    expect(first).toMatchObject({ requestId: "request-401", code: "SESSION_EXPIRED" });
    expect(transport.csrfToken).toBeNull();
    expect(onSessionExpired).toHaveBeenCalledOnce();

    await expect(transport.request("/api/protected")).rejects.toBe(first);
    expect(fetch).toHaveBeenCalledOnce();
  });

  it("keeps the first observed 401 as the single concurrent session latch", async () => {
    const firstResponse = deferred<Response>();
    const secondResponse = deferred<Response>();
    const onSessionExpired = vi.fn();
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockReturnValueOnce(firstResponse.promise)
      .mockReturnValueOnce(secondResponse.promise);
    const transport = new AuthenticatedTransport({ fetch, onSessionExpired });

    const firstRequest = transport.request("/api/first").catch((error: unknown) => error);
    const secondRequest = transport.request("/api/second").catch((error: unknown) => error);
    secondResponse.resolve(
      jsonResponse(
        {
          code: "SESSION_EXPIRED",
          message: "reopen",
          retryable: false,
          request_id: "second-observed",
          details: {},
        },
        { status: 401 },
      ),
    );
    const latched = await secondRequest;
    firstResponse.resolve(
      jsonResponse(
        {
          code: "SESSION_EXPIRED",
          message: "reopen",
          retryable: false,
          request_id: "first-request-late-response",
          details: {},
        },
        { status: 401 },
      ),
    );

    await expect(firstRequest).resolves.toBe(latched);
    await expect(transport.request("/api/after-expiry")).rejects.toBe(latched);
    expect(latched).toMatchObject({ requestId: "second-observed" });
    expect(onSessionExpired).toHaveBeenCalledOnce();
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("delivers a delivery-first latched 401 to a late handler exactly once", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse(
        {
          code: "SESSION_EXPIRED",
          message: "reopen",
          retryable: false,
          request_id: "request-401",
          details: {},
        },
        { status: 401 },
      ),
    );
    const transport = new AuthenticatedTransport({ fetch });

    await expect(
      transport.request("/api/tasks/task-1/delivery"),
    ).rejects.toBeInstanceOf(SessionExpiredError);

    const onSessionExpired = vi.fn();
    const detach = transport.setSessionExpiredHandler(onSessionExpired);
    expect(onSessionExpired).toHaveBeenCalledOnce();

    detach();
    transport.setSessionExpiredHandler(onSessionExpired);
    expect(onSessionExpired).toHaveBeenCalledOnce();
  });
});
