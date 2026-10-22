ALTER TABLE tasks ADD COLUMN execution_id TEXT;
ALTER TABLE tasks ADD COLUMN runner_instance_id TEXT;
ALTER TABLE tasks ADD COLUMN execution_subject_id TEXT;
ALTER TABLE tasks ADD COLUMN claimed_principal_id TEXT;
ALTER TABLE tasks ADD COLUMN claim_policy_id TEXT;
ALTER TABLE tasks ADD COLUMN claim_policy_sha256 TEXT;

CREATE UNIQUE INDEX idx_tasks_unique_execution_id
ON tasks(execution_id)
WHERE execution_id IS NOT NULL;

CREATE TRIGGER tasks_enforce_execution_provenance_on_insert
BEFORE INSERT ON tasks
WHEN NOT (
    (NEW.execution_id IS NULL AND NEW.runner_instance_id IS NULL
     AND NEW.execution_subject_id IS NULL AND NEW.claimed_principal_id IS NULL
     AND NEW.claim_policy_id IS NULL AND NEW.claim_policy_sha256 IS NULL)
    OR
    (NEW.execution_id IS NOT NULL AND NEW.runner_instance_id IS NOT NULL
     AND NEW.execution_subject_id IS NOT NULL AND NEW.claimed_principal_id IS NOT NULL
     AND NEW.claim_policy_id IS NOT NULL AND NEW.claim_policy_sha256 IS NOT NULL)
)
BEGIN
    SELECT RAISE(ABORT, 'partial execution provenance');
END;

CREATE TRIGGER tasks_enforce_execution_provenance_on_update
BEFORE UPDATE ON tasks
WHEN NOT (
    (NEW.execution_id IS NULL AND NEW.runner_instance_id IS NULL
     AND NEW.execution_subject_id IS NULL AND NEW.claimed_principal_id IS NULL
     AND NEW.claim_policy_id IS NULL AND NEW.claim_policy_sha256 IS NULL)
    OR
    (NEW.execution_id IS NOT NULL AND NEW.runner_instance_id IS NOT NULL
     AND NEW.execution_subject_id IS NOT NULL AND NEW.claimed_principal_id IS NOT NULL
     AND NEW.claim_policy_id IS NOT NULL AND NEW.claim_policy_sha256 IS NOT NULL)
)
BEGIN
    SELECT RAISE(ABORT, 'partial execution provenance');
END;
