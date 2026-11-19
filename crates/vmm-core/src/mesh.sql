CREATE TABLE IF NOT EXISTS mesh_exports(
 id TEXT PRIMARY KEY, scope TEXT NOT NULL REFERENCES scopes, container TEXT NOT NULL,
 request TEXT NOT NULL, protocol INTEGER NOT NULL, port INTEGER NOT NULL,
 created INTEGER NOT NULL, instance TEXT NOT NULL, generation TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 1, revoked_by TEXT,
 UNIQUE(scope,request)
);
CREATE TABLE IF NOT EXISTS mesh_imports(
 id TEXT PRIMARY KEY, scope TEXT NOT NULL REFERENCES scopes, container TEXT NOT NULL,
 request TEXT NOT NULL, export TEXT NOT NULL REFERENCES mesh_exports, device TEXT NOT NULL,
 alias TEXT NOT NULL, vip INTEGER NOT NULL, expires INTEGER NOT NULL,
 instance TEXT NOT NULL, generation TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 1, revoked_by TEXT,
 UNIQUE(scope,request)
);
CREATE INDEX IF NOT EXISTS mesh_import_destination ON mesh_imports(export);
