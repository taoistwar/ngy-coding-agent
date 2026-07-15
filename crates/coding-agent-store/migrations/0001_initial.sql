CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE repositories (
    id TEXT PRIMARY KEY,
    selected_path TEXT NOT NULL,
    display_name TEXT NOT NULL,
    git_root TEXT NOT NULL,
    cargo_workspace_root TEXT NOT NULL,
    git_identity_key TEXT NOT NULL,
    cargo_identity_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_opened_at TEXT NOT NULL,
    UNIQUE (git_identity_key, cargo_identity_key)
);

CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    client_request_id TEXT NOT NULL UNIQUE,
    repository_id TEXT NOT NULL REFERENCES repositories(id),
    prompt TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued','running','completed','failed','cancelled','interrupted')),
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    retry_of TEXT UNIQUE REFERENCES tasks(id),
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    last_event_id INTEGER NOT NULL DEFAULT 0 CHECK (last_event_id >= 0),
    failure_json TEXT
);

CREATE TABLE task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_version INTEGER NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    created_at TEXT NOT NULL
);

CREATE INDEX task_events_task_id_id ON task_events(task_id, id);
