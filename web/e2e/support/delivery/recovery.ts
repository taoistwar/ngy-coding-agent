import type { Page, Route } from "@playwright/test";

import type {
  ProcessDeliveryProviderScenario,
  ProcessScenario,
  ScenarioRoots,
  StoreWriterOperation,
} from "../localApp";

const LOST_REPLY_TIMEOUT_MS = 30_000;

export interface LostHttpReply {
  readonly requestBody: unknown;
  readonly status: number;
  readonly responseBody: unknown;
}

export interface ProductionDeliveryScenarioOptions {
  readonly providerScenario: ProcessDeliveryProviderScenario;
  readonly pauseAfterCommit?: {
    readonly name: string;
    readonly operation: StoreWriterOperation;
  };
  readonly pauseBeforeDescriptor?: {
    readonly name: string;
  };
  readonly pauseAfterTerminalDispatch?: {
    readonly name: string;
  };
}

export function productionDeliveryScenario(
  roots: ScenarioRoots,
  options: ProductionDeliveryScenarioOptions,
): ProcessScenario {
  const storePause = options.pauseAfterCommit;
  const recoveryPause = options.pauseBeforeDescriptor;
  const terminalPause = options.pauseAfterTerminalDispatch;
  return {
    runner_mode: {
      kind: "production_offline_delivery",
      repository_path: roots.repositoryDir,
      provider_scenario: options.providerScenario,
      process_fault: "none",
    },
    runtime_config: null,
    fake_scenarios: [],
    storage_samples: [{ kind: "native" }],
    store_writer_faults:
      storePause === undefined
        ? []
        : [
            {
              point: "pause_after_commit_before_wake",
              operation: storePause.operation,
              count: 1,
            },
          ],
    actor_pauses: [
      ...(recoveryPause === undefined ? [] : ["recovery_before_descriptor" as const]),
      ...(terminalPause === undefined
        ? []
        : ["terminal_after_dispatch_before_scheduler_publish" as const]),
    ],
    virtual_release_signals: [
      ...(storePause === undefined
        ? []
        : [
            {
              name: storePause.name,
              path: roots.releaseSignalPath(storePause.name),
              target: "store_writer_after_commit_before_wake" as const,
            },
          ]),
      ...(recoveryPause === undefined
        ? []
        : [
            {
              name: recoveryPause.name,
              path: roots.releaseSignalPath(recoveryPause.name),
              target: "actor_recovery_before_descriptor" as const,
            },
          ]),
      ...(terminalPause === undefined
        ? []
        : [
            {
              name: terminalPause.name,
              path: roots.releaseSignalPath(terminalPause.name),
              target: "actor_terminal_after_dispatch_before_scheduler_publish" as const,
            },
          ]),
    ],
    legacy_v2_seed: { kind: "none" },
    marker_write_failure: false,
  };
}

/**
 * Sends one real browser request to the local server, consumes its response in
 * the harness, then aborts delivery to the page. The server-side command has
 * completed while the UI observes an ambiguous network failure.
 */
export async function loseNextHttpReply(
  page: Page,
  url: string,
  trigger: () => Promise<void>,
  timeoutMs = LOST_REPLY_TIMEOUT_MS,
): Promise<LostHttpReply> {
  let settle: ((value: LostHttpReply) => void) | null = null;
  let fail: ((error: unknown) => void) | null = null;
  const observed = new Promise<LostHttpReply>((resolve, reject) => {
    settle = resolve;
    fail = reject;
  });
  const handler = async (route: Route): Promise<void> => {
    try {
      const requestBody = parseJson(route.request().postData() ?? "null", "delivery request");
      const response = await route.fetch({ timeout: timeoutMs });
      const status = response.status();
      const responseBody = parseJson(await response.text(), "delivery response");
      await route.abort("failed");
      settle?.(Object.freeze({ requestBody, status, responseBody }));
    } catch (error) {
      fail?.(error);
      await route.abort("failed").catch(() => undefined);
    }
  };

  await page.route(url, handler, { times: 1 });
  try {
    await trigger();
    return await withTimeout(observed, timeoutMs, "lost delivery reply");
  } finally {
    await page.unroute(url, handler).catch(() => undefined);
  }
}

function parseJson(text: string, label: string): unknown {
  try {
    return JSON.parse(text) as unknown;
  } catch (cause) {
    throw new Error(`${label} was not JSON`, { cause });
  }
}

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: NodeJS.Timeout | null = null;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer !== null) clearTimeout(timer);
  }
}
