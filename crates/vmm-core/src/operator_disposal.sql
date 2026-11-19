CREATE TABLE IF NOT EXISTS operator_registration_disposals (
    registration TEXT PRIMARY KEY NOT NULL REFERENCES registrations(id),
    request TEXT NOT NULL
);

-- Older schema-2 hosts also honor the permanent fence when reopening this database.
CREATE TRIGGER IF NOT EXISTS disposed_registration_admission
BEFORE UPDATE OF connected, draining ON registrations
WHEN (NEW.connected != 0 OR NEW.draining = 0)
 AND EXISTS (SELECT 1 FROM operator_registration_disposals WHERE registration=NEW.id)
BEGIN
    SELECT RAISE(ABORT, 'registration disposal already accepted');
END;
