import { defineConfig } from "@playwright/test";

const localChromium = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;

export default defineConfig({
  testDir: "./e2e",
  outputDir: "./test-results",
  fullyParallel: false,
  workers: 1,
  forbidOnly: Boolean(process.env.CI),
  retries: 0,
  timeout: 60_000,
  expect: {
    timeout: 10_000,
  },
  reporter: [["list"]],
  use: {
    actionTimeout: 10_000,
    browserName: "chromium",
    headless: true,
    navigationTimeout: 15_000,
    ...(localChromium ? { launchOptions: { executablePath: localChromium } } : {}),
    // A trace would persist the one-time fragment URL passed to page.goto.
    trace: "off",
    screenshot: "only-on-failure",
    video: "off",
  },
});
