import type { Page, Request, WebSocket } from "@playwright/test";

const MAX_REPORTED_ESCAPES = 10;

/**
 * A delivery-test guard stricter than the suite-wide loopback guard: while an
 * app instance is active, browser traffic may only reach that instance's exact
 * origin. A restarted app replaces the allowed origin before it is reopened.
 */
export class ExactAppOriginTrafficGuard {
  private expectedHttpOrigin = "";
  private expectedWebSocketOrigin = "";
  private readonly escapes: string[] = [];
  private escapeCount = 0;
  private disposed = false;

  private readonly onRequest = (request: Request): void => {
    this.inspect(request.url(), "request");
  };

  private readonly onWebSocket = (socket: WebSocket): void => {
    this.inspect(socket.url(), "websocket");
  };

  private constructor(private readonly page: Page, origin: string) {
    this.expectOrigin(origin);
    page.on("request", this.onRequest);
    page.on("websocket", this.onWebSocket);
  }

  static install(page: Page, origin: string): ExactAppOriginTrafficGuard {
    return new ExactAppOriginTrafficGuard(page, origin);
  }

  expectOrigin(origin: string): void {
    const parsed = new URL(origin);
    if (parsed.protocol !== "http:" || parsed.hostname !== "127.0.0.1") {
      throw new Error("delivery browser guard requires an IPv4 loopback HTTP origin");
    }
    if (parsed.pathname !== "/" || parsed.search !== "" || parsed.hash !== "") {
      throw new Error("delivery browser guard requires a bare application origin");
    }
    this.expectedHttpOrigin = parsed.origin;
    this.expectedWebSocketOrigin = `ws://${parsed.host}`;
  }

  assertNoEscapesAndDispose(): void {
    this.dispose();
    if (this.escapeCount === 0) return;
    const omitted = this.escapeCount - this.escapes.length;
    const details = [...this.escapes];
    if (omitted > 0) details.push(`... ${String(omitted)} additional escape(s)`);
    throw new Error(
      `delivery browser traffic escaped the active local app origin: ${details.join(", ")}`,
    );
  }

  private inspect(candidate: string, kind: "request" | "websocket"): void {
    let parsed: URL;
    try {
      parsed = new URL(candidate);
    } catch {
      this.record(kind, "<invalid URL>");
      return;
    }
    if (kind === "request" && parsed.protocol === "data:") return;
    const expected = kind === "request" ? this.expectedHttpOrigin : this.expectedWebSocketOrigin;
    if (parsed.origin === expected) return;
    this.record(kind, parsed.host === "" ? parsed.protocol : `${parsed.protocol}//${parsed.host}`);
  }

  private record(kind: "request" | "websocket", redacted: string): void {
    this.escapeCount += 1;
    if (this.escapes.length < MAX_REPORTED_ESCAPES) {
      this.escapes.push(`${kind} ${redacted}`);
    }
  }

  private dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.page.off("request", this.onRequest);
    this.page.off("websocket", this.onWebSocket);
  }
}
