import { test as base, type Page, type Request, type WebSocket } from "@playwright/test";

import {
  isAllowedLocalBrowserUrl,
  redactBrowserTrafficUrl,
  runGuardedFixtureLifecycle,
  type BrowserTrafficKind,
} from "./support/networkGuard";

export * from "@playwright/test";

const MAX_REPORTED_VIOLATIONS = 20;

interface TrafficViolations {
  examples: Set<string>;
  total: number;
}

export const test = base.extend<{ localTrafficGuard: void }>({
  localTrafficGuard: [
    async ({ context }, use) => {
      const violations: TrafficViolations = { examples: new Set(), total: 0 };
      const observedPages = new Set<Page>();

      const recordViolation = (kind: BrowserTrafficKind, candidate: string): void => {
        if (isAllowedLocalBrowserUrl(candidate, kind)) return;
        violations.total += 1;
        if (violations.examples.size < MAX_REPORTED_VIOLATIONS) {
          violations.examples.add(`${kind} ${redactBrowserTrafficUrl(candidate)}`);
        }
      };
      const onRequest = (request: Request): void => {
        recordViolation("request", request.url());
      };
      const onWebSocket = (socket: WebSocket): void => {
        recordViolation("websocket", socket.url());
      };
      const observePage = (page: Page): void => {
        if (observedPages.has(page)) return;
        observedPages.add(page);
        page.on("websocket", onWebSocket);
      };

      context.on("request", onRequest);
      context.on("page", observePage);
      for (const page of context.pages()) observePage(page);

      await runGuardedFixtureLifecycle({
        body: async () => use(),
        close: async () => context.close(),
        afterClose: () => {
          context.off("request", onRequest);
          context.off("page", observePage);
          for (const page of observedPages) page.off("websocket", onWebSocket);

          if (violations.total === 0) return null;
          const omitted = violations.total - violations.examples.size;
          const detail = [...violations.examples].map((item) => `  - ${item}`);
          if (omitted > 0) detail.push(`  - ... ${omitted} additional violation(s)`);
          return new Error(
            `browser traffic escaped the IPv4 loopback guard:\n${detail.join("\n")}`,
          );
        },
      });
    },
    { auto: true },
  ],
});
