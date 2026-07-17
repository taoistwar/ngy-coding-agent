import type { DiffSnapshot, Task, TestSnapshot, TimelineEntry } from "../api/types";
import { ErrorBoundary } from "./ErrorBoundary";

export interface ResultPaneProps {
  task: Task;
  diff: DiffSnapshot | null;
  tests: TestSnapshot | null;
  timeline: TimelineEntry[];
  boundaryResetKey: string | number;
}

const DIFF_GLYPH: Record<DiffSnapshot["files"][number]["status"], string> = {
  added: "+",
  modified: "±",
  deleted: "−",
};

const TEST_GLYPH: Record<TestSnapshot["status"], string> = {
  queued: "○",
  running: "◐",
  passed: "✓",
  failed: "×",
  cancelled: "−",
};

function DiffPanel({ diff }: { diff: DiffSnapshot | null }) {
  return (
    <section className="evidence-panel diff-panel" aria-labelledby="diff-heading">
      <div className="panel-heading-row">
        <h3 id="diff-heading">Worktree diff</h3>
        {diff !== null ? <span>Revision {diff.revision}</span> : null}
      </div>
      {diff === null || diff.files.length === 0 ? (
        <p className="empty-state">No worktree diff is available yet.</p>
      ) : (
        <ul className="diff-files">
          {diff.files.map((file) => (
            <li key={file.path}>
              <div className="diff-file-heading">
                <span className="status-glyph" aria-hidden="true">
                  {DIFF_GLYPH[file.status]}
                </span>
                <strong>{file.path}</strong>
                <span>
                  {file.status}; +{file.additions} −{file.deletions}
                </span>
                {file.truncated ? (
                  <span className="readonly-badge">Patch truncated at safety limit</span>
                ) : null}
              </div>
              <pre aria-label={`Worktree patch for ${file.path}`}>{file.patch}</pre>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

function TestsPanel({ tests }: { tests: TestSnapshot | null }) {
  return (
    <section className="evidence-panel tests-panel" aria-labelledby="tests-heading">
      <div className="panel-heading-row">
        <h3 id="tests-heading">Test results</h3>
        {tests !== null ? (
          <span className={`status-${tests.status}`}>
            <span className="status-glyph" aria-hidden="true">
              {TEST_GLYPH[tests.status]}
            </span>{" "}
            {tests.status}
          </span>
        ) : null}
      </div>
      {tests === null || tests.cases.length === 0 ? (
        <p className="empty-state">No test results are available yet.</p>
      ) : (
        <ul className="test-cases">
          {tests.cases.map((testCase) => (
            <li key={testCase.id} className={`status-${testCase.status}`}>
              <span className="status-glyph" aria-hidden="true">
                {TEST_GLYPH[testCase.status]}
              </span>
              <span>
                <strong>{testCase.name}</strong>
                <span>
                  {testCase.status} · {testCase.duration_ms} ms
                </span>
                <span>{testCase.summary}</span>
              </span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

function TimelinePanel({ timeline }: { timeline: TimelineEntry[] }) {
  return (
    <section className="evidence-panel timeline-panel" aria-labelledby="timeline-heading">
      <h3 id="timeline-heading">Lifecycle timeline</h3>
      {timeline.length === 0 ? (
        <p className="empty-state">No lifecycle events are available yet.</p>
      ) : (
        <ol>
          {timeline.map((entry) => (
            <li key={entry.event_id}>
              <span className="timeline-marker" aria-hidden="true">
                •
              </span>
              <span>
                <strong>{entry.label}</strong>
                <span>{entry.kind}</span>
                <time dateTime={entry.created_at}>{entry.created_at}</time>
                {entry.failure !== null && entry.failure !== undefined ? (
                  <span className="failure-detail">
                    <code>{entry.failure.code}</code> {entry.failure.message}
                  </span>
                ) : null}
              </span>
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}

export function ResultPane({
  task,
  diff,
  tests,
  timeline,
  boundaryResetKey,
}: ResultPaneProps) {
  return (
    <>
      <h2 id="results-heading">Results and evidence</h2>
      {task.failure !== null && task.failure !== undefined ? (
        <section className="evidence-panel failure-panel" aria-labelledby="failure-heading">
          <h3 id="failure-heading">Failure</h3>
          <p>
            <code>{task.failure.code}</code> {task.failure.message}
          </p>
          <p>{task.failure.retryable ? "Retry is allowed." : "Retry is not advised."}</p>
        </section>
      ) : null}
      <ErrorBoundary
        fallback={<p role="alert">Diff unavailable</p>}
        resetKey={boundaryResetKey}
      >
        <DiffPanel diff={diff} />
      </ErrorBoundary>
      <ErrorBoundary
        fallback={<p role="alert">Test results unavailable</p>}
        resetKey={boundaryResetKey}
      >
        <TestsPanel tests={tests} />
      </ErrorBoundary>
      <ErrorBoundary
        fallback={<p role="alert">Timeline unavailable</p>}
        resetKey={boundaryResetKey}
      >
        <TimelinePanel timeline={timeline} />
      </ErrorBoundary>
    </>
  );
}
