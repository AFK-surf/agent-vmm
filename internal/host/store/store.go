package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const minimumSchema = 39

var schemaMigrations = []string{
	// 40: operator-approved Host Unix sockets an environment's containers reach (host_socket.go).
	`CREATE TABLE host_sockets (
			environment_id TEXT REFERENCES environments(id) ON DELETE CASCADE,
			port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535 AND port != 53), path TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 1, updated_at INTEGER NOT NULL);
CREATE UNIQUE INDEX host_sockets_port ON host_sockets(IFNULL(environment_id, ''), port);`,
}

var schemaVersion = minimumSchema + len(schemaMigrations)

const (
	auditRetention  = 30 * 24 * time.Hour
	auditMaxRows    = 10_000
	auditPruneBatch = 256
)

const (
	OwnerLocal              = 1
	OwnerRemoteRegistration = 2
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	return open(path, schemaMigrations)
}

func open(path string, migrations []string) (*Store, error) {
	target := minimumSchema + len(migrations)
	version, err := ReadSchemaVersion(path)
	if err != nil {
		return nil, err
	}
	if err := CheckSchema(version, minimumSchema, target); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=FULL",
		"PRAGMA wal_autocheckpoint=1000",
		"PRAGMA journal_size_limit=67108864",
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("configure host database: %w", err)
		}
	}
	result := &Store{db: db}
	if version != target {
		if err := result.migrate(context.Background(), version, migrations); err != nil {
			db.Close()
			return nil, err
		}
	}
	if err := result.maintainStorage(context.Background(), time.Now()); err != nil {
		db.Close()
		return nil, fmt.Errorf("prune mutation receipts: %w", err)
	}
	return result, nil
}

func (store *Store) Close() error {
	_, _ = store.db.Exec("PRAGMA wal_checkpoint(PASSIVE)")
	return store.db.Close()
}
