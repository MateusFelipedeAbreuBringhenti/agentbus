ALTER TABLE tasks
ADD COLUMN waiting_on_approval_id TEXT REFERENCES approvals(id);

UPDATE tasks
SET waiting_on_approval_id = (
    SELECT approval.id
    FROM approvals AS approval
    WHERE approval.task_id = tasks.id
      AND approval.correlation_id = tasks.correlation_id
      AND (
          approval.status = 'pending'
          OR 0 = (
              SELECT COUNT(*)
              FROM approvals AS pending_approval
              WHERE pending_approval.task_id = tasks.id
                AND pending_approval.correlation_id = tasks.correlation_id
                AND pending_approval.status = 'pending'
          )
      )
)
WHERE status = 'waiting_approval'
  AND (
      1 = (
          SELECT COUNT(*)
          FROM approvals AS approval
          WHERE approval.task_id = tasks.id
            AND approval.correlation_id = tasks.correlation_id
            AND approval.status = 'pending'
      )
      OR (
          0 = (
              SELECT COUNT(*)
              FROM approvals AS approval
              WHERE approval.task_id = tasks.id
                AND approval.correlation_id = tasks.correlation_id
                AND approval.status = 'pending'
          )
          AND 1 = (
              SELECT COUNT(*)
              FROM approvals AS approval
              WHERE approval.task_id = tasks.id
                AND approval.correlation_id = tasks.correlation_id
          )
      )
  );

CREATE TEMP TABLE task_approval_wait_migration_guard (
    valid INTEGER NOT NULL CHECK (valid = 1)
);

INSERT INTO task_approval_wait_migration_guard(valid)
SELECT CASE WHEN EXISTS (
    SELECT 1
    FROM tasks
    WHERE (status = 'waiting_approval') != (waiting_on_approval_id IS NOT NULL)
) THEN 0 ELSE 1 END;

DROP TABLE task_approval_wait_migration_guard;

CREATE INDEX idx_tasks_waiting_on_approval_id
ON tasks(waiting_on_approval_id);

CREATE TRIGGER tasks_enforce_approval_wait_link_on_insert
BEFORE INSERT ON tasks
WHEN
    (NEW.status = 'waiting_approval') != (NEW.waiting_on_approval_id IS NOT NULL)
    OR (
        NEW.waiting_on_approval_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM approvals
            WHERE id = NEW.waiting_on_approval_id
              AND task_id = NEW.id
              AND correlation_id = NEW.correlation_id
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'invalid task approval wait link');
END;

CREATE TRIGGER tasks_enforce_approval_wait_link_on_update
BEFORE UPDATE ON tasks
WHEN
    (NEW.status = 'waiting_approval') != (NEW.waiting_on_approval_id IS NOT NULL)
    OR (
        NEW.waiting_on_approval_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM approvals
            WHERE id = NEW.waiting_on_approval_id
              AND task_id = NEW.id
              AND correlation_id = NEW.correlation_id
        )
    )
BEGIN
    SELECT RAISE(ABORT, 'invalid task approval wait link');
END;
