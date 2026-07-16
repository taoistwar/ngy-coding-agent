import { readdir, readFile } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

import { expect, test } from "./fixtures";

import { isValidReleaseSignalName } from "./support/localApp";
import {
  isAllowedLocalBrowserUrl,
  redactBrowserTrafficUrl,
  runGuardedFixtureLifecycle,
} from "./support/networkGuard";

const TEST_DIR = dirname(fileURLToPath(import.meta.url));
const PLAYWRIGHT_TEST_FILE = /\.(?:spec|test)\.ts$/u;

test("allows only product loopback protocols plus data requests", () => {
  expect(isAllowedLocalBrowserUrl("data:text/plain,ok", "request")).toBe(true);
  expect(isAllowedLocalBrowserUrl("http://127.0.0.1:4321/api/bootstrap", "request")).toBe(
    true,
  );
  expect(isAllowedLocalBrowserUrl("ws://127.0.0.1:4321/events", "websocket")).toBe(true);

  for (const [candidate, kind] of [
    ["data:text/plain,not-a-socket", "websocket"],
    ["https://127.0.0.1:4321/", "request"],
    ["wss://127.0.0.1:4321/", "websocket"],
    ["http://localhost:4321/", "request"],
    ["http://[::1]:4321/", "request"],
    ["https://example.com/", "request"],
  ] as const) {
    expect(isAllowedLocalBrowserUrl(candidate, kind)).toBe(false);
  }
});

test("redacts paths, credentials, query strings, and fragments from reports", () => {
  const redacted = redactBrowserTrafficUrl(
    "https://user:password@example.com/private/path?token=secret#launch-secret",
  );
  expect(redacted).toBe("https://example.com");
  expect(redacted).not.toContain("private");
  expect(redacted).not.toContain("path");
  expect(redacted).not.toContain("password");
  expect(redacted).not.toContain("secret");
  expect(redactBrowserTrafficUrl("data:text/plain,secret-payload")).toBe("data:<redacted>");
});

test("preserves one fixture error and aggregates body, close, and guard errors", async () => {
  const bodyOnly = new Error("body only");
  await expect(
    runGuardedFixtureLifecycle({
      body: async () => {
        throw bodyOnly;
      },
      close: async () => undefined,
      afterClose: () => null,
    }),
  ).rejects.toBe(bodyOnly);

  const phases: string[] = [];
  const bodyError = new Error("body failed");
  const closeError = new Error("close failed");
  const guardError = new Error("guard failed");
  let combined: unknown;
  try {
    await runGuardedFixtureLifecycle({
      body: async () => {
        phases.push("body");
        throw bodyError;
      },
      close: async () => {
        phases.push("close");
        throw closeError;
      },
      afterClose: () => {
        phases.push("afterClose");
        return guardError;
      },
    });
  } catch (error) {
    combined = error;
  }
  expect(phases).toEqual(["body", "close", "afterClose"]);
  expect(combined).toBeInstanceOf(AggregateError);
  expect((combined as AggregateError).errors).toEqual([bodyError, closeError, guardError]);
});

test("rejects Windows DOS device names for release signals", () => {
  for (const name of [
    "CON",
    "con.trace",
    "PRN",
    "aux.release",
    "NUL",
    "COM1",
    "com9.log",
    "LPT1",
    "lpt9.tmp",
  ]) {
    expect(isValidReleaseSignalName(name), name).toBe(false);
  }
  for (const name of ["console", "com0", "com10", "lpt0", "lpt10", "release-con"]) {
    expect(isValidReleaseSignalName(name), name).toBe(true);
  }
});

test("every Playwright spec or test imports the guarded fixture", async () => {
  expect(PLAYWRIGHT_TEST_FILE.test("sentinel.spec.ts")).toBe(true);
  expect(PLAYWRIGHT_TEST_FILE.test("sentinel.test.ts")).toBe(true);

  const testPaths = await findPlaywrightTestFiles(TEST_DIR);
  const bypasses: string[] = [];

  for (const testPath of testPaths) {
    const source = await readFile(testPath, "utf8");
    if (/from\s+["']@playwright\/test["']/.test(source)) {
      bypasses.push(`${relative(TEST_DIR, testPath)} imports @playwright/test directly`);
    }
    if (!/from\s+["'](?:\.\/|(?:\.\.\/)+)fixtures(?:\.ts)?["']/.test(source)) {
      bypasses.push(`${relative(TEST_DIR, testPath)} does not import the guarded fixture`);
    }
  }

  expect(bypasses).toEqual([]);
});

async function findPlaywrightTestFiles(directory: string): Promise<string[]> {
  const paths: string[] = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      paths.push(...(await findPlaywrightTestFiles(path)));
    } else if (entry.isFile() && PLAYWRIGHT_TEST_FILE.test(entry.name)) {
      paths.push(path);
    }
  }
  return paths.sort();
}
