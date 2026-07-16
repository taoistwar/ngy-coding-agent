export type BrowserTrafficKind = "request" | "websocket";

export interface GuardedFixtureLifecycle {
  body(): Promise<void>;
  close(): Promise<void>;
  afterClose(): Error | null;
}

interface FixtureFailure {
  phase: "body" | "context close" | "traffic guard";
  error: unknown;
}

export async function runGuardedFixtureLifecycle({
  body,
  close,
  afterClose,
}: GuardedFixtureLifecycle): Promise<void> {
  const failures: FixtureFailure[] = [];
  try {
    await body();
  } catch (error) {
    failures.push({ phase: "body", error });
  }

  try {
    await close();
  } catch (error) {
    failures.push({ phase: "context close", error });
  }

  try {
    const guardError = afterClose();
    if (guardError !== null) {
      failures.push({ phase: "traffic guard", error: guardError });
    }
  } catch (error) {
    failures.push({ phase: "traffic guard", error });
  }

  if (failures.length === 1) {
    throw failures[0]?.error;
  }
  if (failures.length > 1) {
    throw new AggregateError(
      failures.map((failure) => failure.error),
      `Playwright fixture failed during ${failures.map((failure) => failure.phase).join(", ")}`,
    );
  }
}

export function isAllowedLocalBrowserUrl(
  candidate: string,
  kind: BrowserTrafficKind,
): boolean {
  let parsed: URL;
  try {
    parsed = new URL(candidate);
  } catch {
    return false;
  }

  if (kind === "request" && parsed.protocol === "data:") {
    return true;
  }

  if (parsed.hostname !== "127.0.0.1") {
    return false;
  }

  return kind === "request"
    ? parsed.protocol === "http:"
    : parsed.protocol === "ws:";
}

export function redactBrowserTrafficUrl(candidate: string): string {
  try {
    const parsed = new URL(candidate);
    if (parsed.host === "") {
      return `${parsed.protocol}<redacted>`;
    }
    return `${parsed.protocol}//${parsed.host}`;
  } catch {
    return "<invalid URL>";
  }
}
