import { cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type {
  DiffSnapshot,
  ReviewEvidence,
  Task,
  TestSnapshot,
} from "../api/types";
import { ReviewPane } from "./ReviewPane";

const NOW = "2026-07-23T00:00:00Z";
const DIGEST_A = "a".repeat(64);
const DIGEST_B = "b".repeat(64);

afterEach(cleanup);

function task(
  status: Task["status"] = "running",
  readiness: Task["delivery_readiness"] = "unreviewed",
): Task {
  return {
    id: "task-1",
    repository_id: "repo-1",
    client_request_id: "request-1",
    prompt: "Implement the quality loop",
    status,
    delivery_readiness: readiness,
    attempt: 1,
    last_event_id: 20,
    created_at: NOW,
    retry_of: null,
    started_at: NOW,
    finished_at: status === "running" ? null : NOW,
    failure: null,
  };
}

function panels(generation: number): {
  diff: DiffSnapshot;
  tests: TestSnapshot;
} {
  return {
    diff: { revision: generation, files: [] },
    tests: { revision: generation, status: "passed", cases: [] },
  };
}

function review(
  round: number,
  verdict: ReviewEvidence["verdict"],
  generation: number,
  overrides: Partial<ReviewEvidence> = {},
): ReviewEvidence {
  return {
    round,
    decision_source: "reviewer",
    workspace_generation: generation,
    workspace_digest: {
      algorithm: "workspace_fingerprint_v1",
      value: generation === 1 ? DIGEST_A : DIGEST_B,
    },
    verdict,
    summary: `Review summary ${round}`,
    findings: [],
    added_required_checks: [],
    required_checks: [
      {
        id: "workspace-tests",
        kind: "cargo_test",
        package: null,
        integration_test: null,
      },
    ],
    check_evidence: [],
    coverage: null,
    created_at: NOW,
    ...overrides,
  };
}

describe("ReviewPane", () => {
  it("renders reviewer and system decisions with findings, checks, coverage, and evidence", () => {
    const selector = `selector-${"long".repeat(40)}`;
    const findingPath = `src/${"nested/".repeat(20)}lib.rs`;
    const first = review(1, "changes_requested", 1, {
      added_required_checks: [
        { id: selector, kind: "cargo_check", package: "coding-agent-app" },
      ],
      required_checks: [
        {
          id: selector,
          kind: "cargo_check",
          package: "coding-agent-app",
        },
        {
          id: "workspace-tests",
          kind: "cargo_test",
          package: null,
          integration_test: null,
        },
      ],
      findings: [
        {
          id: "blocking-1",
          severity: "blocking",
          message: `Finding ${"detail".repeat(30)}`,
          path: findingPath,
          line: 42,
        },
        {
          id: "advisory-1",
          severity: "advisory",
          message: "Consider a smaller helper.",
          path: null,
          line: null,
        },
      ],
      coverage: {
        generation: 1,
        workspace_digest: {
          algorithm: "workspace_fingerprint_v1",
          value: DIGEST_A,
        },
        manifest_sha256: "c".repeat(64),
        covered_chunks: [0, 2],
        total_chunks: 3,
      },
      check_evidence: [
        {
          check_id: "workspace-tests",
          actor: "reviewer",
          role_run: 1,
          workspace_generation: 1,
          workspace_digest: {
            algorithm: "workspace_fingerprint_v1",
            value: DIGEST_A,
          },
          status: "passed",
          duration_ms: 125,
          summary: "All workspace tests passed.",
          truncated: false,
        },
      ],
    });
    const system = review(2, "changes_requested", 2, {
      decision_source: "system",
      summary: "Workspace changed after review.",
      findings: [
        {
          id: "workspace-invalidated",
          severity: "blocking",
          message: "The reviewed workspace digest is no longer current.",
          path: null,
          line: null,
        },
      ],
      required_checks: first.required_checks,
    });

    const { container } = render(
      <ReviewPane task={task()} reviews={[first, system]} {...panels(2)} />,
    );

    expect(screen.getByRole("heading", { name: "Review" })).toBeVisible();
    expect(screen.getByText("Reviewer decision")).toBeVisible();
    expect(screen.getByText("System decision")).toBeVisible();
    const firstRound = within(screen.getByTestId("review-round-1"));
    expect(firstRound.getByText("Blocking findings")).toBeVisible();
    expect(firstRound.getByText("Advisory findings")).toBeVisible();
    expect(firstRound.getByText(`${findingPath}:42`)).toBeVisible();
    expect(firstRound.getByText("Added required checks")).toBeVisible();
    expect(firstRound.getByText("Current required checks")).toBeVisible();
    expect(firstRound.getByText("Diff coverage")).toBeVisible();
    expect(firstRound.getByText("Covered chunks 0, 2 of 3")).toBeVisible();
    expect(firstRound.getByText("Reviewer #1")).toBeVisible();
    expect(firstRound.getByText("125 ms")).toBeVisible();
    expect(container).not.toHaveTextContent(/transcript|reasoning/i);
    expect(
      within(screen.getByTestId("review-round-2")).queryByText("Reviewer decision"),
    ).not.toBeInTheDocument();
  });

  it("marks older rounds historical and treats post-review rework as expected stale", () => {
    const first = review(1, "changes_requested", 1);
    const latest = review(2, "changes_requested", 2);

    render(
      <ReviewPane
        task={task()}
        reviews={[first, latest]}
        {...panels(3)}
      />,
    );

    expect(
      within(screen.getByTestId("review-round-1")).getByText("Historical"),
    ).toBeVisible();
    expect(
      within(screen.getByTestId("review-round-2")).getByText(
        "Expected stale after rework edits",
      ),
    ).toBeVisible();
    expect(screen.queryByText(/integrity warning/i)).not.toBeInTheDocument();
  });

  it("shows pending finalization for an approved review and yields to a terminal snapshot", () => {
    const approved = review(1, "approved", 2);
    const currentPanels = panels(2);
    const { rerender } = render(
      <ReviewPane
        task={task("running", "unreviewed")}
        reviews={[approved]}
        {...currentPanels}
      />,
    );

    expect(
      screen.getByText(
        "Reviewer has decided; waiting for the terminal lifecycle event.",
      ),
    ).toBeVisible();

    rerender(
      <ReviewPane
        task={task("completed", "review_approved")}
        reviews={[approved]}
        {...currentPanels}
      />,
    );

    expect(
      screen.queryByText(
        "Reviewer has decided; waiting for the terminal lifecycle event.",
      ),
    ).not.toBeInTheDocument();
    expect(screen.getByText("Final delivery review")).toBeVisible();
  });

  it("compares only the final delivery review with current workspace panels", () => {
    const historical = review(1, "changes_requested", 1);
    const finalReview = review(2, "approved", 2);

    render(
      <ReviewPane
        task={task("completed", "review_approved")}
        reviews={[historical, finalReview]}
        {...panels(3)}
      />,
    );

    expect(
      within(screen.getByTestId("review-round-1")).queryByText(
        /generation mismatch/i,
      ),
    ).not.toBeInTheDocument();
    expect(
      within(screen.getByTestId("review-round-2")).getByText(
        /Generation mismatch.*review 2.*diff 3.*tests 3/i,
      ),
    ).toBeVisible();
  });

  it("flags missing workspace panels for the final delivery review", () => {
    render(
      <ReviewPane
        task={task("completed", "review_approved")}
        reviews={[review(1, "approved", 2)]}
        diff={null}
        tests={null}
      />,
    );

    expect(
      screen.getByText(
        /Generation mismatch.*review 2.*diff unavailable.*tests unavailable/i,
      ),
    ).toBeVisible();
  });
});
