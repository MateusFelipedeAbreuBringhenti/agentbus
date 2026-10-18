ALTER TABLE events ADD COLUMN authenticated_principal_id TEXT;
ALTER TABLE idempotency_records ADD COLUMN authenticated_principal_id TEXT;

CREATE TRIGGER events_require_authenticated_principal_on_insert
BEFORE INSERT ON events
WHEN NEW.authenticated_principal_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'authenticated principal required for new event');
END;

CREATE TRIGGER idempotency_require_authenticated_principal_on_insert
BEFORE INSERT ON idempotency_records
WHEN NEW.authenticated_principal_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'authenticated principal required for new idempotency record');
END;
