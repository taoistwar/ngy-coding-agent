import type {
  CheckEvidence,
  DiffSnapshot,
  ReviewEvidence,
  ReviewFinding,
  Task,
  TestSnapshot,
  WorkspaceDigest,
} from "../api/types";
import { RequiredChecks } from "./RequiredChecks";

export interface ReviewPaneProps {
  task: Task;
  reviews: ReviewEvidence[];
  diff: DiffSnapshot | null;
  tests: TestSnapshot | null;
}

function shortHash(value: string): string {
  return value.length <= 24
    ? value
    : `${value.slice(0, 12)}…${value.slice(-8)}`;
}

function Digest({ digest }: { digest: WorkspaceDigest }) {
  return (
    <code className="digest-value" title={digest.value}>
      {shortHash(digest.value)}
    </code>
  );
}

function evidenceActor(evidence: CheckEvidence): string {
  return `${evidence.actor === "executor" ? "Executor" : "Reviewer"} #${evidence.role_run}`;
}

function FindingList({
  title,
  findings,
}: {
  title: string;
  findings: ReviewFinding[];
}) {
  if (findings.length === 0) return null;
  return (
    <section className="review-subsection">
      <h5>{title}</h5>
      <ul className="review-finding-list">
        {findings.map((finding) => (
          <li key={finding.id} className={`finding-${finding.severity}`}>
            <p>{finding.message}</p>
            {finding.path !== null ? (
              <code className="finding-location">
                {finding.path}
                {finding.line === null ? "" : `:${finding.line}`}
              </code>
            ) : null}
          </li>
        ))}
      </ul>
    </section>
  );
}

function deliveryMatches(task: Task, review: ReviewEvidence): boolean {
  switch (task.delivery_readiness) {
    case "unreviewed":
      return false;
    case "review_approved":
      return task.status === "completed" && review.verdict === "approved";
    case "review_rejected":
      return (
        task.status === "failed" &&
        review.verdict === "changes_requested" &&
        review.round === 3
      );
  }
}

function generationMessage(
  review: ReviewEvidence,
  diff: DiffSnapshot | null,
  tests: TestSnapshot | null,
): string {
  const diffGeneration = diff?.revision ?? "unavailable";
  const testGeneration = tests?.revision ?? "unavailable";
  return `Generation mismatch: review ${review.workspace_generation}, diff ${diffGeneration}, tests ${testGeneration}.`;
}

function ReviewRound({
  task,
  review,
  historical,
  finalDeliveryReview,
  pendingFinalization,
  expectedStale,
  comparePanels,
  diff,
  tests,
}: {
  task: Task;
  review: ReviewEvidence;
  historical: boolean;
  finalDeliveryReview: boolean;
  pendingFinalization: boolean;
  expectedStale: boolean;
  comparePanels: boolean;
  diff: DiffSnapshot | null;
  tests: TestSnapshot | null;
}) {
  const panelMismatch =
    comparePanels &&
    (diff === null ||
      tests === null ||
      diff.revision !== review.workspace_generation ||
      tests.revision !== review.workspace_generation);
  const blocking = review.findings.filter(
    (finding) => finding.severity === "blocking",
  );
  const advisory = review.findings.filter(
    (finding) => finding.severity === "advisory",
  );

  return (
    <article
      className={`review-round verdict-${review.verdict} source-${review.decision_source}`}
      data-testid={`review-round-${review.round}`}
    >
      <div className="review-round-heading">
        <h4>Round {review.round}</h4>
        <span className={`review-verdict verdict-${review.verdict}`}>
          {review.verdict === "approved" ? "Approved" : "Changes requested"}
        </span>
      </div>
      <div className="review-round-badges" aria-label={`Review round ${review.round} state`}>
        {historical ? <span>Historical</span> : null}
        {expectedStale ? <span>Expected stale after rework edits</span> : null}
        {finalDeliveryReview ? <span>Final delivery review</span> : null}
        {pendingFinalization ? <span>Awaiting terminal lifecycle</span> : null}
      </div>
      <dl className="review-meta">
        <div>
          <dt>Decision source</dt>
          <dd>
            {review.decision_source === "system"
              ? "System decision"
              : "Reviewer decision"}
          </dd>
        </div>
        <div>
          <dt>Workspace generation</dt>
          <dd>{review.workspace_generation}</dd>
        </div>
        <div>
          <dt>Workspace digest</dt>
          <dd>
            <Digest digest={review.workspace_digest} />
          </dd>
        </div>
      </dl>
      {pendingFinalization ? (
        <p className="pending-finalization" role="status">
          Reviewer has decided; waiting for the terminal lifecycle event.
        </p>
      ) : null}
      {panelMismatch && !expectedStale ? (
        <p className="generation-warning" role="alert">
          {generationMessage(review, diff, tests)}
        </p>
      ) : null}
      {task.delivery_readiness !== "unreviewed" &&
      !historical &&
      !finalDeliveryReview ? (
        <p className="generation-warning" role="alert">
          Delivery readiness does not match the latest review evidence in this
          snapshot.
        </p>
      ) : null}
      <p className="review-summary">{review.summary}</p>

      {review.findings.length === 0 ? (
        <p className="empty-state">No findings were recorded.</p>
      ) : (
        <>
          <FindingList title="Blocking findings" findings={blocking} />
          <FindingList title="Advisory findings" findings={advisory} />
        </>
      )}

      <section className="review-subsection">
        <h5>Added required checks</h5>
        <RequiredChecks
          checks={review.added_required_checks}
          emptyMessage="No required checks were added in this round."
        />
      </section>
      <section className="review-subsection">
        <h5>Current required checks</h5>
        <RequiredChecks
          checks={review.required_checks}
          emptyMessage="No required checks were recorded."
        />
      </section>

      <section className="review-subsection">
        <h5>Diff coverage</h5>
        {review.coverage === null ? (
          <p className="empty-state">No diff coverage was recorded.</p>
        ) : (
          <dl className="coverage-details">
            <div>
              <dt>Workspace generation</dt>
              <dd>{review.coverage.generation}</dd>
            </div>
            <div>
              <dt>Workspace digest</dt>
              <dd>
                <Digest digest={review.coverage.workspace_digest} />
              </dd>
            </div>
            <div>
              <dt>Manifest digest</dt>
              <dd>
                <code title={review.coverage.manifest_sha256}>
                  {shortHash(review.coverage.manifest_sha256)}
                </code>
              </dd>
            </div>
          </dl>
        )}
        {review.coverage !== null ? (
          <p className="coverage-chunks">
            Covered chunks{" "}
            {review.coverage.covered_chunks.length === 0
              ? "none"
              : review.coverage.covered_chunks.join(", ")}{" "}
            of {review.coverage.total_chunks}
          </p>
        ) : null}
      </section>

      <section className="review-subsection">
        <h5>Check evidence</h5>
        {review.check_evidence.length === 0 ? (
          <p className="empty-state">No check evidence was recorded.</p>
        ) : (
          <ul className="check-evidence-list">
            {review.check_evidence.map((evidence) => (
              <li
                key={`${evidence.check_id}:${evidence.actor}:${evidence.role_run}`}
                className={`check-status-${evidence.status}`}
              >
                <div className="check-evidence-heading">
                  <code>{evidence.check_id}</code>
                  <strong>{evidence.status}</strong>
                </div>
                <span>{evidenceActor(evidence)}</span>
                <span>Workspace generation {evidence.workspace_generation}</span>
                <span>
                  Digest <Digest digest={evidence.workspace_digest} />
                </span>
                <span>{evidence.duration_ms} ms</span>
                <p>{evidence.summary}</p>
                {evidence.truncated ? (
                  <span className="readonly-badge">
                    Evidence summary truncated at safety limit
                  </span>
                ) : null}
              </li>
            ))}
          </ul>
        )}
      </section>
      <time dateTime={review.created_at}>{review.created_at}</time>
    </article>
  );
}

export function ReviewPane({
  task,
  reviews,
  diff,
  tests,
}: ReviewPaneProps) {
  const latestIndex = reviews.length - 1;
  const panelGenerations = [
    ...(diff === null ? [] : [diff.revision]),
    ...(tests === null ? [] : [tests.revision]),
  ];

  return (
    <section className="evidence-panel review-panel" aria-labelledby="review-heading">
      <h3 id="review-heading">Review</h3>
      {reviews.length === 0 ? (
        <p className="empty-state">No review evidence is available yet.</p>
      ) : (
        <div className="review-rounds">
          {reviews.map((review, index) => {
            const historical = index < latestIndex;
            const finalDeliveryReview =
              index === latestIndex && deliveryMatches(task, review);
            const pendingFinalization =
              index === latestIndex &&
              task.status === "running" &&
              task.delivery_readiness === "unreviewed" &&
              review.verdict === "approved";
            const expectedStale =
              index === latestIndex &&
              task.status === "running" &&
              task.delivery_readiness === "unreviewed" &&
              review.verdict === "changes_requested" &&
              panelGenerations.some(
                (generation) => generation > review.workspace_generation,
              );
            const currentChangesRequested =
              index === latestIndex &&
              task.status === "running" &&
              task.delivery_readiness === "unreviewed" &&
              review.verdict === "changes_requested" &&
              !expectedStale;
            const comparePanels =
              !historical &&
              (finalDeliveryReview ||
                pendingFinalization ||
                currentChangesRequested);

            return (
              <ReviewRound
                key={review.round}
                task={task}
                review={review}
                historical={historical}
                finalDeliveryReview={finalDeliveryReview}
                pendingFinalization={pendingFinalization}
                expectedStale={expectedStale}
                comparePanels={comparePanels}
                diff={diff}
                tests={tests}
              />
            );
          })}
        </div>
      )}
    </section>
  );
}
