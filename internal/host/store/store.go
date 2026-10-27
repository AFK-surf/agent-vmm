package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const schemaVersion = 39

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
	version, err := ReadSchemaVersion(path)
	if err != nil {
		return nil, err
	}
	if err := checkSchema(version); err != nil {
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
	if version == 0 {
		if err := result.initialize(context.Background()); err != nil {
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
