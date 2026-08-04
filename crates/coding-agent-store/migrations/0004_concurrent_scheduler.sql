CREATE TABLE task_stop_intents (
    task_id TEXT PRIMARY KEY NOT NULL
        CHECK (typeof(task_id) = 'text' AND length(task_id) > 0),
    repository_id TEXT NOT NULL
        CHECK (typeof(repository_id) = 'text' AND length(repository_id) > 0),
    attempt INTEGER NOT NULL
        CHECK (typeof(attempt) = 'integer' AND attempt > 0),
    kind TEXT NOT NULL
        CHECK (
            typeof(kind) = 'text'
            AND kind IN ('user_cancelled', 'disk_pressure_critical')
        ),
    requested_at TEXT NOT NULL
        CHECK (typeof(requested_at) = 'text' AND length(requested_at) > 0),
    FOREIGN KEY (task_id, repository_id, attempt)
        REFERENCES tasks (id, repository_id, attempt)
) STRICT;

CREATE TRIGGER task_stop_intents_running_unreviewed_on_insert
BEFORE INSERT ON task_stop_intents
WHEN NOT EXISTS (
    SELECT 1
    FROM tasks t
    LEFT JOIN task_delivery_state d ON d.task_id = t.id
    WHERE t.id = NEW.task_id
      AND t.repository_id = NEW.repository_id
      AND t.attempt = NEW.attempt
      AND t.status = 'running'
      AND typeof(t.started_at) = 'text'
      AND length(t.started_at) > 0
      AND t.finished_at IS NULL
      AND t.failure_json IS NULL
      AND d.task_id IS NULL
)
BEGIN
    SELECT RAISE(ABORT, 'stop intent requires a running unreviewed task');
END;

CREATE TRIGGER task_stop_intents_no_replace
BEFORE INSERT ON task_stop_intents
WHEN EXISTS (
    SELECT 1
    FROM task_stop_intents
    WHERE task_id = NEW.task_id
)
BEGIN
    SELECT RAISE(ABORT, 'task_stop_intents is immutable');
END;

CREATE TRIGGER task_stop_intents_no_update
BEFORE UPDATE ON task_stop_intents
BEGIN
    SELECT RAISE(ABORT, 'task_stop_intents is immutable');
END;

CREATE TRIGGER task_stop_intents_no_delete
BEFORE DELETE ON task_stop_intents
BEGIN
    SELECT RAISE(ABORT, 'task_stop_intents is immutable');
END;

CREATE TRIGGER tasks_stop_intent_no_replace
BEFORE INSERT ON tasks
WHEN EXISTS (
    SELECT 1
    FROM task_stop_intents i
    WHERE i.task_id = NEW.id
)
BEGIN
    SELECT RAISE(ABORT, 'task row replacement conflicts with stop intent');
END;

CREATE TRIGGER tasks_stop_intent_identity_collision_on_update
BEFORE UPDATE ON tasks
WHEN OLD.id != NEW.id
    AND EXISTS (
        SELECT 1
        FROM task_stop_intents i
        WHERE i.task_id = NEW.id
    )
BEGIN
    SELECT RAISE(ABORT, 'task identity update conflicts with stop intent');
END;

CREATE TRIGGER tasks_stop_intent_terminal_on_update
BEFORE UPDATE ON tasks
WHEN EXISTS (
    SELECT 1
    FROM task_stop_intents i
    WHERE i.task_id = OLD.id
      AND (
          NEW.id != i.task_id
          OR NEW.repository_id != i.repository_id
          OR NEW.attempt != i.attempt
          OR (
              OLD.status != 'running'
              AND (
                  NEW.status IS NOT OLD.status
                  OR NEW.started_at IS NOT OLD.started_at
                  OR NEW.finished_at IS NOT OLD.finished_at
                  OR NEW.failure_json IS NOT OLD.failure_json
              )
          )
          OR NOT (
              (
                  NEW.status = 'running'
                  AND NEW.started_at IS OLD.started_at
                  AND NEW.finished_at IS OLD.finished_at
                  AND NEW.failure_json IS OLD.failure_json
                  AND NOT EXISTS (
                      SELECT 1
                      FROM task_delivery_state d
                      WHERE d.task_id = NEW.id
                  )
              )
              OR (
                  i.kind = 'user_cancelled'
                  AND NEW.status = 'cancelled'
                  AND NEW.started_at IS OLD.started_at
                  AND typeof(NEW.finished_at) = 'text'
                  AND length(NEW.finished_at) > 0
                  AND NEW.failure_json IS NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM task_delivery_state d
                      WHERE d.task_id = NEW.id
                  )
              )
              OR (
                  i.kind = 'disk_pressure_critical'
                  AND NEW.status = 'failed'
                  AND NEW.started_at IS OLD.started_at
                  AND typeof(NEW.finished_at) = 'text'
                  AND length(NEW.finished_at) > 0
                  AND typeof(NEW.failure_json) = 'text'
                  AND json_valid(NEW.failure_json)
                  AND json_type(NEW.failure_json) = 'object'
                  AND json_type(NEW.failure_json, '$.code') = 'text'
                  AND json_extract(NEW.failure_json, '$.code')
                      = 'DISK_PRESSURE_CRITICAL'
                  AND json_type(NEW.failure_json, '$.message') = 'text'
                  AND length(json_extract(NEW.failure_json, '$.message')) > 0
                  AND json_type(NEW.failure_json, '$.retryable') = 'true'
                  AND (
                      SELECT COUNT(*)
                      FROM json_each(NEW.failure_json)
                  ) = 3
                  AND NOT EXISTS (
                      SELECT 1
                      FROM json_each(NEW.failure_json)
                      WHERE key NOT IN ('code', 'message', 'retryable')
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM task_delivery_state d
                      WHERE d.task_id = NEW.id
                  )
              )
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'task terminal state conflicts with stop intent');
END;

CREATE TRIGGER task_review_evidence_stop_intent_on_insert
BEFORE INSERT ON task_review_evidence
WHEN EXISTS (
    SELECT 1 FROM task_stop_intents i WHERE i.task_id = NEW.task_id
)
BEGIN
    SELECT RAISE(ABORT, 'review evidence conflicts with stop intent');
END;

CREATE TRIGGER task_delivery_state_stop_intent_on_insert
BEFORE INSERT ON task_delivery_state
WHEN EXISTS (
    SELECT 1 FROM task_stop_intents i WHERE i.task_id = NEW.task_id
)
BEGIN
    SELECT RAISE(ABORT, 'delivery finalization conflicts with stop intent');
END;

CREATE TRIGGER task_events_stop_intent_review_on_insert
BEFORE INSERT ON task_events
WHEN NEW.kind = 'review.updated'
    AND EXISTS (
        SELECT 1 FROM task_stop_intents i WHERE i.task_id = NEW.task_id
    )
BEGIN
    SELECT RAISE(ABORT, 'review event conflicts with stop intent');
END;

CREATE TRIGGER task_events_stop_intent_review_on_update
BEFORE UPDATE OF task_id, kind ON task_events
WHEN NEW.kind = 'review.updated'
    AND EXISTS (
        SELECT 1 FROM task_stop_intents i WHERE i.task_id = NEW.task_id
    )
BEGIN
    SELECT RAISE(ABORT, 'review event conflicts with stop intent');
END;

CREATE INDEX tasks_queued_created_at_id
    ON tasks (created_at, id)
    WHERE status = 'queued';
