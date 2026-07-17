CREATE UNIQUE INDEX tasks_id_repository_attempt
    ON tasks (id, repository_id, attempt);

CREATE TABLE task_attempt_artifacts (
    task_id TEXT PRIMARY KEY NOT NULL REFERENCES tasks(id),
    repository_id TEXT NOT NULL REFERENCES repositories(id),
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    base_commit TEXT NOT NULL
        CHECK (
            length(base_commit) IN (40, 64)
            AND base_commit NOT GLOB '*[^0-9a-f]*'
        ),
    branch_name TEXT NOT NULL CHECK (length(branch_name) > 0),
    worktree_path TEXT NOT NULL CHECK (length(worktree_path) > 0),
    state TEXT NOT NULL CHECK (state IN ('reserved', 'ready', 'inconsistent')),
    failure_code TEXT,
    created_at TEXT NOT NULL CHECK (length(created_at) > 0),
    updated_at TEXT NOT NULL CHECK (length(updated_at) > 0),
    UNIQUE (branch_name),
    UNIQUE (worktree_path),
    UNIQUE (repository_id, task_id, attempt),
    FOREIGN KEY (task_id, repository_id, attempt)
        REFERENCES tasks (id, repository_id, attempt),
    CHECK (
        (state IN ('reserved', 'ready') AND failure_code IS NULL)
        OR (state = 'inconsistent' AND length(failure_code) > 0)
    )
);
