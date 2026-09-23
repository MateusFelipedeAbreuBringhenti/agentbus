CREATE TABLE approvals (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    gate TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'rejected')),
    request_reason TEXT NOT NULL,
    context_json TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    assigned_to TEXT,
    decided_by TEXT,
    decision_reason TEXT,
    correlation_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    decided_at TEXT
);

CREATE INDEX idx_approvals_task_id ON approvals(task_id);
CREATE INDEX idx_approvals_correlation_id ON approvals(correlation_id);

CREATE UNIQUE INDEX idx_approvals_one_pending_gate
ON approvals(task_id, gate)
WHERE status = 'pending';
