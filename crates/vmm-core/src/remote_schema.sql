ALTER TABLE registrations ADD COLUMN inventory_watermark INTEGER NOT NULL DEFAULT 1;
ALTER TABLE scopes ADD COLUMN remote_id TEXT;
ALTER TABLE scopes ADD COLUMN display_label TEXT NOT NULL DEFAULT '';
ALTER TABLE scopes ADD COLUMN disposal_request TEXT;

CREATE UNIQUE INDEX remote_scope_identity ON scopes(registration, COALESCE(remote_id, id));
CREATE TRIGGER remote_inventory_insert AFTER INSERT ON scopes
WHEN NEW.registration IS NOT NULL
BEGIN
    UPDATE registrations SET inventory_watermark=inventory_watermark+1 WHERE id=NEW.registration;
END;
CREATE TRIGGER remote_inventory_update AFTER UPDATE OF data, remote_id, display_label ON scopes
WHEN NEW.registration IS NOT NULL AND
     (NEW.data IS NOT OLD.data OR NEW.remote_id IS NOT OLD.remote_id OR NEW.display_label IS NOT OLD.display_label)
BEGIN
    UPDATE registrations SET inventory_watermark=inventory_watermark+1 WHERE id=NEW.registration;
END;

PRAGMA user_version=2;
