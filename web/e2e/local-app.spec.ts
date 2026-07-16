import { writeFile } from "node:fs/promises";

import { expect, test, type Page, type TestInfo } from "./fixtures";

import { successScenario, withLocalApp, type LocalApp } from "./support/localApp";

interface ExchangeLocation {
  href: string;
  hash: string;
  historyLength: number;
}

interface TransportEvent {
  atEpochMs: number;
  atPerformanceMs: number;
  phase: string;
  path: string;
  method?: string;
  status?: number;
  bytes?: number;
  eventName?: string;
  eventId?: string;
  taskStatus?: string;
  eventCursor?: number;
  lastEventId?: number;
}

interface StatusTransition {
  atEpochMs: number;
  atPerformanceMs: number;
  workspace: string | null;
  sidebar: string[];
}

declare global {
  interface Window {
    __codingAgentE2EExchangeLocation?: ExchangeLocation;
    __codingAgentE2ETransportEvents?: TransportEvent[];
    __codingAgentE2EStatusTransitions?: StatusTransition[];
  }
}

const TASK_PROMPT = "Complete the deterministic E2E task";

test("clears launch token before exchange and keeps every request local", async ({ page }, testInfo) => {
  await withLocalApp(testInfo, successScenario(), async (app) => {
    const traffic = observeLocalTraffic(page, app.origin);
    const exchange = await openAndObserveExchange(page, app, () => app.open(page));

    expect(exchange.requestUrl).toBe(`${app.origin}/api/session/exchange`);
    expect(exchange.requestMethod).toBe("POST");
    expect(exchange.location.hash).toBe("");
    expect(exchange.location.href).toBe(`${app.origin}/`);
    expect(exchange.location.href).not.toContain("token=");
    expect(exchange.bootstrapStatus).toBe(200);
    expect(exchange.bootstrapCors).toBeNull();
    await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();
    await expect(page.getByRole("status")).toContainText("Connected");

    if (exchange.location.historyLength > 1) {
      await page.goBack({ waitUntil: "domcontentloaded" });
      expect(page.url()).not.toContain("#token=");
      await page.goForward({ waitUntil: "domcontentloaded" });
      await expect(page.getByRole("heading", { name: "New task" })).toBeVisible();
      expect(page.url()).toBe(`${app.origin}/`);
      expect(page.url()).not.toContain("token=");
    }

    expect(traffic.violations).toEqual([]);
    await quitThroughUi(page, app);
    expect(traffic.violations).toEqual([]);
  });
});

test("adds a real repository, completes a task, and reopens the persisted workspace", async ({
  context,
  page,
}, testInfo) => {
  await withLocalApp(testInfo, successScenario(), async (app) => {
    const firstTraffic = observeLocalTraffic(page, app.origin);
    await openAndObserveExchange(page, app, () => app.open(page));
    const repositoryBeforeDiscovery = await app.repositorySnapshot();

    await page.getByLabel("Repository path").fill(app.repositoryDir);
    const repositoryResponse = page.waitForResponse(
      (response) =>
        response.request().method() === "POST" &&
        response.url() === `${app.origin}/api/repositories`,
    );
    await page.getByRole("button", { name: "Add repository path" }).click();
    expect((await repositoryResponse).status()).toBe(201);
    await expect(
      page.getByRole("button", { name: /^fixture-repository /u }),
    ).toBeVisible();
    expect(await app.repositorySnapshot()).toEqual(repositoryBeforeDiscovery);

    await installStatusProbe(page);
    await page.getByLabel("Task description").fill(TASK_PROMPT);
    const completionWaitStartedAt = Date.now();
    await page.getByRole("button", { name: "Create task" }).click();
    try {
      await expect(page.getByText("Status: completed", { exact: true })).toBeVisible({
        timeout: 30_000,
      });
    } catch (error) {
      const diagnostic = await attachBrowserDiagnostics(
        page,
        testInfo,
        completionWaitStartedAt,
      );
      const summary = JSON.stringify({
        assertionStartedAt: diagnostic.assertionStartedAt,
        capturedAt: diagnostic.capturedAt,
        currentWorkspaceStatus: diagnostic.currentWorkspaceStatus,
        currentSidebarStatuses: diagnostic.currentSidebarStatuses,
        statusTransitions: diagnostic.statusTransitions,
        transportTail: diagnostic.transportEvents.slice(-40),
      });
      throw new Error(`live completion was not projected in time; diagnostic=${summary}`, {
        ...(error instanceof Error ? { cause: error } : {}),
      });
    }
    await expect(page.getByText("Execution completed — not reviewed", { exact: true })).toBeVisible();
    await expect(page.getByText("Prepare deterministic plan", { exact: true })).toBeVisible();
    await expect(page.getByText("Generated synthetic diff", { exact: true })).toBeVisible();
    await expect(page.getByText("Synthetic checks passed", { exact: true })).toBeVisible();
    expect(firstTraffic.violations).toEqual([]);

    await page.close();
    const reopenedPage = await context.newPage();
    const reopenedTraffic = observeLocalTraffic(reopenedPage, app.origin);
    const reopenedExchange = await openAndObserveExchange(reopenedPage, app, () =>
      app.reopen(reopenedPage),
    );
    expect(reopenedExchange.location.href).toBe(`${app.origin}/`);
    expect(reopenedExchange.location.hash).toBe("");

    await reopenedPage
      .getByRole("button", { name: `${TASK_PROMPT} Attempt 1`, exact: true })
      .click();
    await expect(
      reopenedPage.getByText("Execution completed — not reviewed", { exact: true }),
    ).toBeVisible();
    await expect(reopenedPage.getByText("Synthetic checks passed", { exact: true })).toBeVisible();
    expect(reopenedTraffic.violations).toEqual([]);

    await quitThroughUi(reopenedPage, app);
    expect(reopenedTraffic.violations).toEqual([]);
  });
});

async function installExchangeProbe(page: Page): Promise<void> {
  await page.addInitScript(() => {
    delete window.__codingAgentE2EExchangeLocation;
    window.__codingAgentE2ETransportEvents = [];
    const record = (
      event: Omit<TransportEvent, "atEpochMs" | "atPerformanceMs">,
    ) => {
      const events = window.__codingAgentE2ETransportEvents ?? [];
      events.push({
        atEpochMs: Date.now(),
        atPerformanceMs: performance.now(),
        ...event,
      });
      if (events.length > 1_000) events.shift();
      window.__codingAgentE2ETransportEvents = events;
    };
    const isRecord = (value: unknown): value is Record<string, unknown> =>
      typeof value === "object" && value !== null && !Array.isArray(value);
    const observeJsonProjection = async (
      response: Response,
      path: string,
      method: string,
    ) => {
      const isTaskDetail = method === "GET" && /^\/api\/tasks\/[^/]+$/u.test(path);
      const isCreateTask = method === "POST" && path === "/api/tasks";
      const isBootstrap = method === "GET" && path === "/api/bootstrap";
      if (!isTaskDetail && !isCreateTask && !isBootstrap) return;
      try {
        const candidate = (await response.json()) as unknown;
        if (!isRecord(candidate)) return;
        if (isTaskDetail && isRecord(candidate.task)) {
          record({
            phase: "json_projection",
            path,
            method,
            ...(typeof candidate.task.status === "string"
              ? { taskStatus: candidate.task.status }
              : {}),
            ...(typeof candidate.event_cursor === "number"
              ? { eventCursor: candidate.event_cursor }
              : {}),
            ...(typeof candidate.task.last_event_id === "number"
              ? { lastEventId: candidate.task.last_event_id }
              : {}),
          });
          return;
        }
        if (isCreateTask) {
          record({
            phase: "json_projection",
            path,
            method,
            ...(typeof candidate.status === "string" ? { taskStatus: candidate.status } : {}),
            ...(typeof candidate.last_event_id === "number"
              ? { lastEventId: candidate.last_event_id }
              : {}),
          });
          return;
        }
        record({
          phase: "json_projection",
          path,
          method,
          ...(typeof candidate.latest_event_id === "number"
            ? { eventCursor: candidate.latest_event_id }
            : {}),
        });
      } catch {
        record({ phase: "json_projection_error", path, method });
      }
    };
    const observeSse = async (response: Response, path: string) => {
      if (response.body === null) return;
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let pending = "";
      try {
        while (true) {
          const next = await reader.read();
          if (next.done) break;
          record({ phase: "sse_chunk", path, bytes: next.value.byteLength });
          pending += decoder.decode(next.value, { stream: true });
          while (true) {
            const boundary = /\r?\n\r?\n/u.exec(pending);
            if (boundary === null || boundary.index === undefined) break;
            const frame = pending.slice(0, boundary.index);
            pending = pending.slice(boundary.index + boundary[0].length);
            const lines = frame.split(/\r?\n/u);
            const eventName = lines
              .find((line) => line.startsWith("event:"))
              ?.slice("event:".length)
              .trim();
            const eventId = lines
              .find((line) => line.startsWith("id:"))
              ?.slice("id:".length)
              .trim();
            record({
              phase: "sse_frame",
              path,
              eventName: eventName ?? (frame.startsWith(":") ? "heartbeat" : "message"),
              ...(eventId === undefined ? {} : { eventId }),
            });
          }
        }
        record({ phase: "sse_end", path });
      } catch {
        record({ phase: "sse_read_error", path });
      }
    };
    const nativeFetch = window.fetch.bind(window);
    window.fetch = async (...arguments_: Parameters<typeof window.fetch>) => {
      const [input] = arguments_;
      const requestUrl =
        typeof input === "string"
          ? new URL(input, window.location.href)
          : input instanceof URL
            ? new URL(input.href, window.location.href)
            : new URL(input.url, window.location.href);
      const method = (
        arguments_[1]?.method ?? (input instanceof Request ? input.method : "GET")
      ).toUpperCase();
      const safePath = requestUrl.pathname;
      if (requestUrl.pathname === "/api/session/exchange") {
        window.__codingAgentE2EExchangeLocation = {
          href: window.location.href,
          hash: window.location.hash,
          historyLength: window.history.length,
        };
      }
      record({ phase: "fetch_request", path: safePath, method });
      try {
        const response = await nativeFetch(...arguments_);
        record({
          phase: "fetch_response",
          path: safePath,
          method,
          status: response.status,
        });
        if (
          requestUrl.pathname === "/api/bootstrap" ||
          requestUrl.pathname === "/api/tasks" ||
          /^\/api\/tasks\/[^/]+$/u.test(requestUrl.pathname)
        ) {
          void observeJsonProjection(response.clone(), requestUrl.pathname, method);
        }
        if (requestUrl.pathname === "/api/events") {
          void observeSse(response.clone(), safePath);
        }
        return response;
      } catch (error) {
        record({ phase: "fetch_error", path: safePath, method });
        throw error;
      }
    };
  });
}

async function installStatusProbe(page: Page): Promise<void> {
  await page.evaluate(() => {
    window.__codingAgentE2EStatusTransitions = [];
    let previous = "";
    const sample = () => {
      const workspace = document.querySelector(".task-status")?.textContent?.trim() ?? null;
      const sidebar = Array.from(document.querySelectorAll(".task-list-status"), (element) =>
        (element.textContent ?? "").trim(),
      );
      const signature = JSON.stringify({ workspace, sidebar });
      if (signature === previous) return;
      previous = signature;
      const transitions = window.__codingAgentE2EStatusTransitions ?? [];
      transitions.push({
        atEpochMs: Date.now(),
        atPerformanceMs: performance.now(),
        workspace,
        sidebar,
      });
      window.__codingAgentE2EStatusTransitions = transitions;
    };
    const observer = new MutationObserver(sample);
    observer.observe(document.body, { childList: true, characterData: true, subtree: true });
    sample();
  });
}

async function attachBrowserDiagnostics(
  page: Page,
  testInfo: TestInfo,
  assertionStartedAt: number,
): Promise<{
  assertionStartedAt: number;
  capturedAt: number;
  currentWorkspaceStatus: string | null;
  currentSidebarStatuses: string[];
  statusTransitions: StatusTransition[];
  transportEvents: TransportEvent[];
}> {
  const diagnostic = await page.evaluate(
    ({ startedAt }) => ({
      assertionStartedAt: startedAt,
      capturedAt: Date.now(),
      currentWorkspaceStatus:
        document.querySelector(".task-status")?.textContent?.trim() ?? null,
      currentSidebarStatuses: Array.from(
        document.querySelectorAll(".task-list-status"),
        (element) => (element.textContent ?? "").trim(),
      ),
      statusTransitions: window.__codingAgentE2EStatusTransitions ?? [],
      transportEvents: window.__codingAgentE2ETransportEvents ?? [],
    }),
    { startedAt: assertionStartedAt },
  );
  const body = `${JSON.stringify(diagnostic, null, 2)}\n`;
  const diagnosticPath = testInfo.outputPath("browser-live-timeline.json");
  await writeFile(diagnosticPath, body, { encoding: "utf8", mode: 0o600 });
  await testInfo.attach("browser-live-timeline.json", {
    path: diagnosticPath,
    contentType: "application/json",
  });
  return diagnostic;
}

async function openAndObserveExchange(
  page: Page,
  app: LocalApp,
  navigate: () => Promise<void>,
): Promise<{
  requestUrl: string;
  requestMethod: string;
  location: ExchangeLocation;
  bootstrapStatus: number;
  bootstrapCors: string | null;
}> {
  await installExchangeProbe(page);
  const exchangeRequest = page.waitForRequest(
    (request) => request.url() === `${app.origin}/api/session/exchange`,
  );
  const bootstrapResponse = page.waitForResponse(
    (response) => response.url() === `${app.origin}/api/bootstrap`,
  );
  // Register rejection handlers before navigation so a failed page load cannot
  // leave either observer as an unhandled promise rejection.
  void exchangeRequest.catch(() => undefined);
  void bootstrapResponse.catch(() => undefined);

  await navigate();
  const exchange = await exchangeRequest;
  const bootstrap = await bootstrapResponse;
  const location = await page.evaluate(
    () => window.__codingAgentE2EExchangeLocation ?? null,
  );
  if (location === null) {
    throw new Error("the exchange fetch was not observed by the pre-navigation probe");
  }
  return {
    requestUrl: exchange.url(),
    requestMethod: exchange.method(),
    location,
    bootstrapStatus: bootstrap.status(),
    bootstrapCors: await bootstrap.headerValue("access-control-allow-origin"),
  };
}

function observeLocalTraffic(page: Page, origin: string): { violations: string[] } {
  const violations: string[] = [];
  page.on("request", (request) => {
    if (!isAllowedPageUrl(request.url(), origin, false)) {
      violations.push(`request ${safeUrl(request.url())}`);
    }
  });
  page.on("websocket", (socket) => {
    if (!isAllowedPageUrl(socket.url(), origin, true)) {
      violations.push(`websocket ${safeUrl(socket.url())}`);
    }
  });
  return { violations };
}

function isAllowedPageUrl(candidate: string, origin: string, websocket: boolean): boolean {
  if (!websocket && candidate.startsWith("data:")) return true;
  try {
    const parsed = new URL(candidate);
    const expected = new URL(origin);
    if (parsed.hostname !== "127.0.0.1" || parsed.port !== expected.port) return false;
    return websocket ? parsed.protocol === "ws:" : parsed.protocol === "http:" && parsed.origin === origin;
  } catch {
    return false;
  }
}

function safeUrl(candidate: string): string {
  try {
    const parsed = new URL(candidate);
    return `${parsed.protocol}//${parsed.host}${parsed.pathname}`;
  } catch {
    return "<invalid URL>";
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
