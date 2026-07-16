import { randomUUID } from "node:crypto";
import { access } from "node:fs/promises";
import path from "node:path";

import {
  expect,
  test,
  type APIResponse,
  type Page,
  type Request,
} from "./fixtures";

import { successScenario, withLocalApp, type LocalApp } from "./support/localApp";

test("rejects unauthorized local requests before protected capabilities run", async (
  { context, page, request },
  testInfo,
) => {
  await withLocalApp(testInfo, successScenario(), async (app) => {
    const exchangeRequest = page.waitForRequest(isSessionExchange);
    const exchangeResponse = page.waitForResponse(
      (response) => isSessionExchange(response.request()),
    );

    await app.open(page);
    const [initialExchangeRequest, initialExchangeResponse] = await Promise.all([
      exchangeRequest,
      exchangeResponse,
    ]);
    expect(initialExchangeResponse.status()).toBe(204);
    expect(corsHeader(initialExchangeResponse.headers())).toBeUndefined();

    const replayBody = sessionExchangeBody(initialExchangeRequest);
    await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();

    const bootstrapResponse = await context.request.get(`${app.origin}/api/bootstrap`);
    expect(bootstrapResponse.status()).toBe(200);
    expect(corsHeader(bootstrapResponse.headers())).toBeUndefined();
    const csrfToken = csrfFromBootstrap(await bootstrapResponse.json());

    const mutationUrl = `${app.origin}/api/tasks/${randomUUID()}/cancel`;

    await test.step("rejects a wrong Host before session authorization", async () => {
      await expectSecurityError(
        context.request.get(`${app.origin}/api/bootstrap`, {
          headers: { host: `localhost:${app.port}` },
        }),
        403,
        "SECURITY_INVALID_HOST",
      );
    });

    await test.step("rejects missing and forged session cookies", async () => {
      await expectSecurityError(
        request.get(`${app.origin}/api/bootstrap`),
        401,
        "SECURITY_INVALID_SESSION",
      );
      await expectSecurityError(
        request.get(`${app.origin}/api/bootstrap`, {
          headers: { cookie: "coding_agent_session=forged" },
        }),
        401,
        "SECURITY_INVALID_SESSION",
      );
    });

    await test.step("rejects missing and wrong mutation origins", async () => {
      await expectSecurityError(
        context.request.post(mutationUrl, {
          headers: { "x-csrf-token": csrfToken },
        }),
        403,
        "SECURITY_INVALID_ORIGIN",
      );
      await expectSecurityError(
        context.request.post(mutationUrl, {
          headers: {
            origin: "https://evil.invalid",
            "x-csrf-token": csrfToken,
          },
        }),
        403,
        "SECURITY_INVALID_ORIGIN",
      );
    });

    await test.step("rejects missing and wrong CSRF under the exact origin", async () => {
      await expectSecurityError(
        context.request.post(mutationUrl, {
          headers: { origin: app.origin },
        }),
        403,
        "SECURITY_INVALID_CSRF",
      );
      await expectSecurityError(
        context.request.post(mutationUrl, {
          headers: {
            origin: app.origin,
            "x-csrf-token": "forged-csrf-token",
          },
        }),
        403,
        "SECURITY_INVALID_CSRF",
      );
    });

    await test.step("rejects missing and wrong launcher secrets", async () => {
      await expectSecurityError(
        request.post(`${app.origin}/_local/reopen`),
        401,
        "SECURITY_INVALID_LAUNCHER_SECRET",
      );
      await expectSecurityError(
        request.post(`${app.origin}/_local/reopen`, {
          headers: { "x-launcher-secret": "forged-launcher-secret" },
        }),
        401,
        "SECURITY_INVALID_LAUNCHER_SECRET",
      );
    });

    await test.step("rejects replay of the launch token after the real exchange", async () => {
      await expectSecurityError(
        context.request.post(`${app.origin}/api/session/exchange`, {
          data: replayBody,
          headers: { origin: app.origin },
        }),
        401,
        "SECURITY_INVALID_LAUNCH_TOKEN",
      );
    });

    await test.step("rejects an unauthenticated picker before native dispatch", async () => {
      const pickerProbe = path.join(app.runtimeDir, "native-picker-invoked.probe");
      expect(await pathExists(pickerProbe)).toBe(false);
      await expectSecurityError(
        request.post(`${app.origin}/api/repositories/pick`, {
          headers: {
            origin: app.origin,
            "x-csrf-token": csrfToken,
          },
        }),
        401,
        "SECURITY_INVALID_SESSION",
      );
      expect(await pathExists(pickerProbe)).toBe(false);

      const authorized = await context.request.post(`${app.origin}/api/repositories/pick`, {
        headers: {
          origin: app.origin,
          "x-csrf-token": csrfToken,
        },
      });
      expect(authorized.status()).toBe(204);
      expect(corsHeader(authorized.headers())).toBeUndefined();
      expect(await pathExists(pickerProbe)).toBe(true);
    });

    await quitThroughUi(page, app);
  });
});

function isSessionExchange(request: Request): boolean {
  return (
    request.method() === "POST" && new URL(request.url()).pathname === "/api/session/exchange"
  );
}

function sessionExchangeBody(request: Request): { token: string } {
  const body = request.postDataJSON() as unknown;
  if (!isRecord(body) || typeof body.token !== "string" || body.token.length === 0) {
    throw new Error("the real browser exchange did not carry one non-empty launch token");
  }
  return { token: body.token };
}

async function expectSecurityError(
  responsePromise: Promise<APIResponse>,
  expectedStatus: number,
  expectedCode: string,
): Promise<void> {
  const response = await responsePromise;
  expect(response.status()).toBe(expectedStatus);
  expect(corsHeader(response.headers())).toBeUndefined();

  const candidate = (await response.json()) as unknown;
  if (!isRecord(candidate)) {
    throw new Error("security rejection did not return a JSON object");
  }
  expect(candidate.code).toBe(expectedCode);
  expect(typeof candidate.request_id).toBe("string");
  if (typeof candidate.request_id !== "string") {
    throw new Error("security rejection did not include a string request ID");
  }
  expect(candidate.request_id.trim()).not.toBe("");
}

function csrfFromBootstrap(candidate: unknown): string {
  if (
    !isRecord(candidate) ||
    typeof candidate.csrf_token !== "string" ||
    candidate.csrf_token.length === 0
  ) {
    throw new Error("authorized bootstrap did not include a non-empty CSRF token");
  }
  return candidate.csrf_token;
}

function corsHeader(headers: Record<string, string>): string | undefined {
  return headers["access-control-allow-origin"];
}

async function pathExists(candidate: string): Promise<boolean> {
  try {
    await access(candidate);
    return true;
  } catch {
    return false;
  }
}

async function quitThroughUi(page: Page, app: LocalApp): Promise<void> {
  await page.getByRole("button", { name: "Quit local application" }).click();
  const dialog = page.getByRole("dialog", { name: "Quit local application?" });
  await expect(dialog).toBeVisible();
  await Promise.all([
    app.waitForCleanExit(),
    dialog.getByRole("button", { name: "Quit application" }).click(),
  ]);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
