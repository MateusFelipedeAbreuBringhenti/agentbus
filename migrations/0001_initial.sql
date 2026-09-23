CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    input_json TEXT NOT NULL,
    output_json TEXT,
    status TEXT NOT NULL CHECK (
        status IN (
            'ready',
            'running',
            'waiting_approval',
            'succeeded',
            'failed',
            'cancelled'
        )
    ),
    requested_by TEXT NOT NULL,
    assigned_to TEXT,
    failure_code TEXT,
    failure_message TEXT,
    retry_of TEXT REFERENCES tasks(id),
    correlation_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);

CREATE INDEX idx_tasks_correlation_id ON tasks(correlation_id);

CREATE TABLE events (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    type TEXT NOT NULL,
    data_json TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_event_id TEXT REFERENCES events(id),
    command_id TEXT NOT NULL,
    event_index INTEGER NOT NULL CHECK (event_index >= 0),
    occurred_at TEXT NOT NULL,
    UNIQUE(task_id, sequence),
    UNIQUE(command_id, event_index)
);

CREATE INDEX idx_events_correlation_id ON events(correlation_id);

CREATE TRIGGER events_are_append_only_on_update
BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;

CREATE TRIGGER events_are_append_only_on_delete
BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;

CREATE TABLE idempotency_records (
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    command_id TEXT NOT NULL UNIQUE,
    correlation_id TEXT NOT NULL,
    causation_event_id TEXT REFERENCES events(id),
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    response_status INTEGER NOT NULL,
    response_body TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, key)
);
