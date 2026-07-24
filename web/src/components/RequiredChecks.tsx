import type { RequiredCheck } from "../api/types";

export interface RequiredChecksProps {
  checks: RequiredCheck[];
  emptyMessage: string;
}

export function requiredCheckSelector(check: RequiredCheck): string {
  const packageSelector =
    check.package === null ? "" : ` --package ${check.package}`;
  if (check.kind === "cargo_check") {
    return `cargo check${packageSelector}`;
  }
  const integrationSelector =
    check.integration_test === null ? "" : ` --test ${check.integration_test}`;
  return `cargo test${packageSelector}${integrationSelector}`;
}

export function RequiredChecks({
  checks,
  emptyMessage,
}: RequiredChecksProps) {
  if (checks.length === 0) {
    return <p className="empty-state">{emptyMessage}</p>;
  }

  return (
    <ul className="required-check-list">
      {checks.map((check) => (
        <li key={check.id}>
          <code className="check-id">{check.id}</code>
          <code className="check-selector">{requiredCheckSelector(check)}</code>
        </li>
      ))}
    </ul>
  );
}
