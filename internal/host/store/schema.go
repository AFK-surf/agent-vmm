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
const MigrationFailed = "STATE_MIGRATION_FAILED"

type SchemaError struct {
	Code       string
	DataSchema int
	message    string
}

func (err *SchemaError) Error() string { return err.message }

func CurrentSchema() int { return schemaVersion }
func MinimumSchema() int { return minimumSchema }

const StartFailureFile = "host-start-failure.json"

type StartFailure struct {
	Code          string `json:"code"`
	DataSchema    int    `json:"dataSchema"`
	CurrentSchema int    `json:"currentSchema"`
}

// CheckSchema describes the target database decoder, not the executable version.
// Lifecycle uses it before stopping the current Host. Open enforces it before writes.
func CheckSchema(version, minimum, current int) error {
	if version == current && current > 0 {
		return nil
	}
	if minimum <= 0 || current < minimum || version != 0 && (version < minimum || version > current) {
		return &SchemaError{Code: SchemaIncompatible, DataSchema: version,
			message: fmt.Sprintf("host database schema %d cannot migrate to schema %d (minimum %d); keep the current Host or reset this installation", version, current, minimum)}
	}
	return nil
}

// ReadSchemaVersion reads the schema of an existing database without changing
// it. Only a missing or empty database is fresh (schema 0).
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
	var tables, markers int
	if err := db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(name='schema_migrations'), 0) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables, &markers); err != nil {
		return 0, err
	}
	if tables == 0 {
		return 0, nil
	}
	var version int
	if markers != 0 {
		if err := db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&version); err != nil {
			return 0, err
		}
	}
	if version == 0 {
		return 0, &SchemaError{Code: SchemaIncompatible, message: "existing Host database has no schema version; keep the data or reset this installation"}
	}
	return version, nil
}

// Migrations contain database-only SQL, in version order from minimumSchema.
// The Host owner lock excludes another Host. One SQLite transaction commits all
// pending changes and their version records together. Failure leaves the source
// schema intact; recovery belongs to lifecycle, never this store.
func (store *Store) migrate(ctx context.Context, version int, migrations []string) (err error) {
	target := minimumSchema + len(migrations)
	source := version
	defer func() {
		if err != nil {
			err = &SchemaError{Code: MigrationFailed, DataSchema: source,
				message: fmt.Sprintf("migrate Host database from schema %d to %d: %v", source, target, err)}
		}
	}()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if version == 0 {
		if _, err = tx.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
			return err
		}
		if _, err = tx.Exec(currentSchema); err != nil {
			return err
		}
		version = target
		if _, err = tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", version, time.Now().UnixNano()); err != nil {
			return err
		}
	}
	for version < target {
		if _, err = tx.Exec(migrations[version-minimumSchema]); err != nil {
			return err
		}
		version++
		if _, err = tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", version, time.Now().UnixNano()); err != nil {
			return err
		}
	}
	return tx.Commit()
}
