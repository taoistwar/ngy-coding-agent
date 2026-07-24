CREATE UNIQUE INDEX task_events_id_task_id_kind
    ON task_events (id, task_id, kind);

CREATE TABLE task_review_evidence (
    task_id TEXT NOT NULL CHECK (typeof(task_id) = 'text' AND length(task_id) > 0),
    repository_id TEXT NOT NULL
        CHECK (typeof(repository_id) = 'text' AND length(repository_id) > 0),
    attempt INTEGER NOT NULL CHECK (typeof(attempt) = 'integer' AND attempt > 0),
    review_round INTEGER NOT NULL
        CHECK (typeof(review_round) = 'integer' AND review_round BETWEEN 1 AND 3),
    workspace_generation INTEGER NOT NULL
        CHECK (
            typeof(workspace_generation) = 'integer'
            AND workspace_generation BETWEEN 0 AND 9007199254740991
        ),
    digest_algorithm TEXT NOT NULL
        CHECK (
            typeof(digest_algorithm) = 'text'
            AND digest_algorithm = 'workspace_fingerprint_v1'
        ),
    workspace_digest TEXT NOT NULL
        CHECK (
            typeof(workspace_digest) = 'text'
            AND length(workspace_digest) = 64
            AND workspace_digest NOT GLOB '*[^0-9a-f]*'
        ),
    decision_source TEXT NOT NULL
        CHECK (
            typeof(decision_source) = 'text'
            AND decision_source IN ('reviewer', 'system')
        ),
    verdict TEXT NOT NULL
        CHECK (
            typeof(verdict) = 'text'
            AND verdict IN ('approved', 'changes_requested')
        ),
    summary TEXT NOT NULL
        CHECK (
            typeof(summary) = 'text'
            AND length(CAST(summary AS BLOB)) > 0
        ),
    findings_json TEXT NOT NULL
        CHECK (
            typeof(findings_json) = 'text'
            AND json_valid(findings_json)
            AND json_type(findings_json) = 'array'
            AND json_array_length(findings_json) BETWEEN 0 AND 32
        ),
    added_checks_json TEXT NOT NULL
        CHECK (
            typeof(added_checks_json) = 'text'
            AND json_valid(added_checks_json)
            AND json_type(added_checks_json) = 'array'
            AND json_array_length(added_checks_json) BETWEEN 0 AND 16
        ),
    required_checks_json TEXT NOT NULL
        CHECK (
            typeof(required_checks_json) = 'text'
            AND json_valid(required_checks_json)
            AND json_type(required_checks_json) = 'array'
            AND json_array_length(required_checks_json) BETWEEN 1 AND 16
        ),
    check_evidence_json TEXT NOT NULL
        CHECK (
            typeof(check_evidence_json) = 'text'
            AND json_valid(check_evidence_json)
            AND json_type(check_evidence_json) = 'array'
            AND json_array_length(check_evidence_json) BETWEEN 0 AND 16
        ),
    coverage_json TEXT NOT NULL
        CHECK (
            typeof(coverage_json) = 'text'
            AND json_valid(coverage_json)
            AND json_type(coverage_json) IN ('null', 'object')
        ),
    created_at TEXT NOT NULL
        CHECK (typeof(created_at) = 'text' AND length(created_at) > 0),
    event_id INTEGER NOT NULL
        CHECK (typeof(event_id) = 'integer' AND event_id > 0),
    event_kind TEXT NOT NULL
        CHECK (typeof(event_kind) = 'text' AND event_kind = 'review.updated'),
    PRIMARY KEY (task_id, review_round),
    UNIQUE (task_id, review_round, verdict),
    UNIQUE (event_id),
    FOREIGN KEY (task_id, repository_id, attempt)
        REFERENCES tasks (id, repository_id, attempt),
    FOREIGN KEY (event_id, task_id, event_kind)
        REFERENCES task_events (id, task_id, kind),
    CHECK (decision_source != 'system' OR verdict = 'changes_requested'),
    CHECK (
        length(CAST(json_object(
            'round', review_round,
            'decision_source', decision_source,
            'workspace_generation', workspace_generation,
            'workspace_digest', json_object(
                'algorithm', digest_algorithm,
                'value', workspace_digest
            ),
            'verdict', verdict,
            'summary', summary,
            'findings', json(findings_json),
            'added_required_checks', json(added_checks_json),
            'required_checks', json(required_checks_json),
            'check_evidence', json(check_evidence_json),
            'coverage', json(coverage_json),
            'created_at', created_at
        ) AS BLOB)) <= 131072
    )
) STRICT;

CREATE TABLE task_delivery_state (
    task_id TEXT PRIMARY KEY NOT NULL
        CHECK (typeof(task_id) = 'text' AND length(task_id) > 0),
    readiness TEXT NOT NULL
        CHECK (
            typeof(readiness) = 'text'
            AND readiness IN ('review_approved', 'review_rejected')
        ),
    final_review_round INTEGER NOT NULL
        CHECK (
            typeof(final_review_round) = 'integer'
            AND final_review_round BETWEEN 1 AND 3
        ),
    final_verdict TEXT NOT NULL
        CHECK (
            typeof(final_verdict) = 'text'
            AND final_verdict IN ('approved', 'changes_requested')
        ),
    decided_at TEXT NOT NULL
        CHECK (typeof(decided_at) = 'text' AND length(decided_at) > 0),
    FOREIGN KEY (task_id, final_review_round, final_verdict)
        REFERENCES task_review_evidence (task_id, review_round, verdict),
    CHECK (
        (readiness = 'review_approved' AND final_verdict = 'approved')
        OR (
            readiness = 'review_rejected'
            AND final_verdict = 'changes_requested'
            AND final_review_round = 3
        )
    )
) STRICT;

CREATE TRIGGER tasks_reviewed_terminal_on_insert
BEFORE INSERT ON tasks
WHEN NEW.status = 'completed'
    OR (
        NEW.status = 'failed'
        AND json_valid(NEW.failure_json)
        AND json_extract(NEW.failure_json, '$.code') = 'REVIEW_REJECTED'
    )
BEGIN
    SELECT RAISE(ABORT, 'reviewed terminal tasks require finalization');
END;

CREATE TRIGGER tasks_reviewed_terminal_on_update
BEFORE UPDATE OF status, failure_json ON tasks
WHEN (
        NEW.status = 'completed'
        AND OLD.status != 'completed'
        AND NOT EXISTS (
            SELECT 1 FROM task_delivery_state d
            WHERE d.task_id = NEW.id AND d.readiness = 'review_approved'
        )
    ) OR (
        NEW.status = 'failed'
        AND json_valid(NEW.failure_json)
        AND json_extract(NEW.failure_json, '$.code') = 'REVIEW_REJECTED'
        AND NOT EXISTS (
            SELECT 1 FROM task_delivery_state d
            WHERE d.task_id = NEW.id AND d.readiness = 'review_rejected'
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'reviewed terminal tasks require finalization');
END;

CREATE TRIGGER task_review_evidence_no_replace
BEFORE INSERT ON task_review_evidence
WHEN EXISTS (
    SELECT 1
    FROM task_review_evidence
    WHERE (
        task_id = NEW.task_id
        AND review_round = NEW.review_round
    ) OR event_id = NEW.event_id
)
BEGIN
    SELECT RAISE(ABORT, 'task_review_evidence is immutable');
END;

CREATE TRIGGER task_review_evidence_no_update
BEFORE UPDATE ON task_review_evidence
BEGIN
    SELECT RAISE(ABORT, 'task_review_evidence is immutable');
END;

CREATE TRIGGER task_review_evidence_no_delete
BEFORE DELETE ON task_review_evidence
BEGIN
    SELECT RAISE(ABORT, 'task_review_evidence is immutable');
END;

CREATE TRIGGER task_delivery_state_no_replace
BEFORE INSERT ON task_delivery_state
WHEN EXISTS (
    SELECT 1
    FROM task_delivery_state
    WHERE task_id = NEW.task_id
)
BEGIN
    SELECT RAISE(ABORT, 'task_delivery_state is immutable');
END;

CREATE TRIGGER task_delivery_state_no_update
BEFORE UPDATE ON task_delivery_state
BEGIN
    SELECT RAISE(ABORT, 'task_delivery_state is immutable');
END;

CREATE TRIGGER task_delivery_state_no_delete
BEFORE DELETE ON task_delivery_state
BEGIN
    SELECT RAISE(ABORT, 'task_delivery_state is immutable');
END;

CREATE TRIGGER task_events_review_marker_on_insert
BEFORE INSERT ON task_events
WHEN NEW.kind = 'review.updated'
    AND (
        NEW.schema_version != 1
        OR NEW.payload_json != '{"evidence_ref":true}'
    )
BEGIN
    SELECT RAISE(ABORT, 'review.updated requires the evidence marker');
END;

CREATE TRIGGER task_events_review_marker_on_update
BEFORE UPDATE OF schema_version, kind, payload_json ON task_events
WHEN (OLD.kind = 'review.updated' OR NEW.kind = 'review.updated')
    AND (
        NEW.kind != 'review.updated'
        OR NEW.schema_version != 1
        OR NEW.payload_json != '{"evidence_ref":true}'
    )
BEGIN
    SELECT RAISE(ABORT, 'review.updated requires the evidence marker');
END;

CREATE TRIGGER task_events_review_no_update
BEFORE UPDATE ON task_events
WHEN OLD.kind = 'review.updated'
BEGIN
    SELECT RAISE(ABORT, 'review.updated events are immutable');
END;

CREATE TRIGGER task_events_review_no_delete
BEFORE DELETE ON task_events
WHEN OLD.kind = 'review.updated'
BEGIN
    SELECT RAISE(ABORT, 'review.updated events are immutable');
END;
