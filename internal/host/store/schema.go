package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"
)

const SchemaIncompatible = "STATE_SCHEMA_INCOMPATIBLE"

type SchemaError struct {
	Code       string
	DataSchema int
	message    string
}

func (err *SchemaError) Error() string { return err.message }

func CurrentSchema() int { return schemaVersion }

const StartFailureFile = "host-start-failure.json"

type StartFailure struct {
	Code          string `json:"code"`
	DataSchema    int    `json:"dataSchema"`
	CurrentSchema int    `json:"currentSchema"`
}

func checkSchema(version int) error {
	if version != 0 && version != schemaVersion {
		return &SchemaError{Code: SchemaIncompatible, DataSchema: version,
			message: fmt.Sprintf("host database schema %d is incompatible with this Host (schema %d); run agent-vmm-lifecycle uninstall --purge, then install again", version, schemaVersion)}
	}
	return nil
}

// ReadSchemaVersion reads the schema of an existing database without changing
// it. A missing database or migration table is a fresh database, schema 0.
func ReadSchemaVersion(path string) (int, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}).String())
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var tables int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'`).Scan(&tables); err != nil || tables == 0 {
		return 0, err
	}
	var version int
	err = db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&version)
	return version, err
}

func (store *Store) initialize(ctx context.Context) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		return err
	}
	if _, err := tx.Exec(currentSchema); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", schemaVersion, time.Now().UnixNano()); err != nil {
		return err
	}
	return tx.Commit()
}
