PRAGMA application_id=0x564d4d43;
PRAGMA user_version=1;

CREATE TABLE registrations (
    id TEXT PRIMARY KEY NOT NULL, epoch BLOB NOT NULL CHECK(length(epoch)=8),
    connected INTEGER NOT NULL DEFAULT 0, draining INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE appliances (
    id TEXT PRIMARY KEY NOT NULL, registration TEXT NOT NULL UNIQUE REFERENCES registrations, data TEXT NOT NULL
);
CREATE TABLE scopes (
    id TEXT PRIMARY KEY NOT NULL, registration TEXT REFERENCES registrations, data TEXT NOT NULL
);
CREATE TABLE sessions (
    id TEXT PRIMARY KEY NOT NULL, scope TEXT NOT NULL REFERENCES scopes,
    capability BLOB NOT NULL UNIQUE, revoked INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX current_session ON sessions(scope) WHERE revoked=0;

CREATE TABLE images (
    digest BLOB PRIMARY KEY NOT NULL CHECK(length(digest)=32),
    artifact TEXT, phase TEXT NOT NULL CHECK(phase IN ('installing','ready','discarding'))
);
CREATE TABLE image_refs (
    scope TEXT NOT NULL REFERENCES scopes, reference TEXT NOT NULL, digest BLOB NOT NULL REFERENCES images,
    PRIMARY KEY(scope,reference)
);
CREATE INDEX image_ref_digest ON image_refs(digest);
CREATE TABLE volumes (
    scope TEXT NOT NULL REFERENCES scopes, id TEXT NOT NULL, data TEXT NOT NULL, phase TEXT NOT NULL,
    PRIMARY KEY(scope,id)
);
CREATE TABLE sandboxes (
    scope TEXT NOT NULL REFERENCES scopes, id TEXT NOT NULL, generation TEXT NOT NULL,
    digest BLOB REFERENCES images, definition TEXT NOT NULL, phase TEXT NOT NULL,
    boot TEXT, vm TEXT, exit_reason TEXT,
    PRIMARY KEY(scope,id,generation)
);
CREATE UNIQUE INDEX current_sandbox ON sandboxes(scope,id) WHERE phase!='deleted';
CREATE INDEX sandbox_image ON sandboxes(digest);
CREATE TABLE mounts (
    scope TEXT NOT NULL, sandbox TEXT NOT NULL, generation TEXT NOT NULL,
    volume TEXT NOT NULL, writable INTEGER NOT NULL,
    PRIMARY KEY(scope,sandbox,generation,volume),
    FOREIGN KEY(scope,sandbox,generation) REFERENCES sandboxes(scope,id,generation),
    FOREIGN KEY(scope,volume) REFERENCES volumes(scope,id)
);
CREATE INDEX mount_owners ON mounts(scope,volume);
CREATE TABLE service_exports (id TEXT PRIMARY KEY NOT NULL, data TEXT NOT NULL);
CREATE TABLE routes (
    revision INTEGER PRIMARY KEY AUTOINCREMENT CHECK(revision>0),
    id TEXT NOT NULL, binding TEXT NOT NULL, phase TEXT NOT NULL
);
CREATE UNIQUE INDEX current_route ON routes(id) WHERE phase!='revoked';
CREATE TABLE receipts (
    domain TEXT NOT NULL, scope TEXT NOT NULL, request TEXT NOT NULL, fingerprint BLOB NOT NULL,
    state TEXT NOT NULL, response BLOB NOT NULL,
    PRIMARY KEY(domain,scope,request)
);
CREATE TABLE reset_fences (
    domain TEXT NOT NULL, scope TEXT NOT NULL, request TEXT NOT NULL,
    PRIMARY KEY(domain,scope,request)
);
