-- The same named resource can be removed and recreated with identical properties.
CREATE TABLE IF NOT EXISTS volume_generations (
    scope TEXT NOT NULL, id TEXT NOT NULL, generation TEXT NOT NULL,
    PRIMARY KEY(scope,id), FOREIGN KEY(scope,id) REFERENCES volumes ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS image_ref_generations (
    scope TEXT NOT NULL, reference TEXT NOT NULL, generation TEXT NOT NULL,
    PRIMARY KEY(scope,reference), FOREIGN KEY(scope,reference) REFERENCES image_refs ON DELETE CASCADE
);
CREATE TRIGGER IF NOT EXISTS volume_generation_on_create AFTER INSERT ON volumes BEGIN
    INSERT INTO volume_generations VALUES(NEW.scope,NEW.id,lower(hex(randomblob(16))));
END;
CREATE TRIGGER IF NOT EXISTS image_generation_on_create AFTER INSERT ON image_refs BEGIN
    INSERT INTO image_ref_generations VALUES(NEW.scope,NEW.reference,lower(hex(randomblob(16))));
END;
INSERT INTO volume_generations
    SELECT scope,id,lower(hex(randomblob(16))) FROM volumes v
    WHERE NOT EXISTS (SELECT 1 FROM volume_generations g WHERE g.scope=v.scope AND g.id=v.id);
INSERT INTO image_ref_generations
    SELECT scope,reference,lower(hex(randomblob(16))) FROM image_refs i
    WHERE NOT EXISTS (SELECT 1 FROM image_ref_generations g WHERE g.scope=i.scope AND g.reference=i.reference);
CREATE TABLE IF NOT EXISTS environment_cleanups (
    id TEXT PRIMARY KEY NOT NULL, request TEXT NOT NULL UNIQUE, scope TEXT NOT NULL REFERENCES scopes,
    discard INTEGER NOT NULL, revision INTEGER NOT NULL, complete INTEGER NOT NULL DEFAULT 0,
    started_at INTEGER NOT NULL DEFAULT (unixepoch()), updated_at INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE UNIQUE INDEX IF NOT EXISTS active_environment_cleanup ON environment_cleanups(scope) WHERE complete=0;
