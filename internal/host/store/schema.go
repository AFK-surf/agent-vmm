package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (store *Store) migrate(ctx context.Context) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at INTEGER NOT NULL
	)`); err != nil {
		return err
	}
	var version int
	err = tx.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&version)
	if err != nil {
		return err
	}
	if version > schemaVersion {
		return fmt.Errorf("host database schema %d is newer than supported %d", version, schemaVersion)
	}
	if version == 0 {
		if err := migrateV1(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (1, ?)", time.Now().UnixNano()); err != nil {
			return err
		}
		version = 1
	}
	if version == 1 {
		if err := migrateV2(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (2, ?)", time.Now().UnixNano()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func migrateV1(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE vm_config (
			id INTEGER PRIMARY KEY CHECK (id = 1), instance_id TEXT NOT NULL,
			bundle_directory TEXT NOT NULL, instance_directory TEXT NOT NULL,
			state_disk_identity TEXT NOT NULL, cpu_count INTEGER NOT NULL,
			memory_bytes INTEGER NOT NULL, state_bytes INTEGER NOT NULL
		)`,
		`CREATE TABLE environments (
			id TEXT PRIMARY KEY, namespace_id TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL,
			cpu_millis INTEGER NOT NULL, memory_bytes INTEGER NOT NULL, pids INTEGER NOT NULL,
			disk_bytes INTEGER NOT NULL, egress_mode INTEGER NOT NULL,
			policy_revision INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE sessions (
			id TEXT PRIMARY KEY, environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
			socket_path TEXT NOT NULL UNIQUE, expires_at INTEGER NOT NULL, revoked_at INTEGER
		)`,
		`CREATE TABLE grants (
			id TEXT PRIMARY KEY, kind TEXT NOT NULL, source_namespace_id TEXT NOT NULL,
			target_namespace_id TEXT NOT NULL, methods TEXT NOT NULL, expires_at INTEGER,
			revoked_at INTEGER, revision INTEGER NOT NULL DEFAULT 1
		)`,
		`CREATE TABLE volume_policies (
			volume_id TEXT NOT NULL, owner_namespace_id TEXT NOT NULL REFERENCES environments(namespace_id) ON DELETE CASCADE,
			revision INTEGER NOT NULL DEFAULT 1,
			PRIMARY KEY (owner_namespace_id, volume_id)
		)`,
		`CREATE TABLE operations (
			id TEXT PRIMARY KEY, kind TEXT NOT NULL, environment_id TEXT NOT NULL,
			state INTEGER NOT NULL, stage TEXT NOT NULL, request_id TEXT NOT NULL UNIQUE,
			error_reason TEXT NOT NULL DEFAULT '', error_message TEXT NOT NULL DEFAULT '', output_path TEXT NOT NULL DEFAULT '',
			started_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE audit (
			sequence INTEGER PRIMARY KEY AUTOINCREMENT, actor TEXT NOT NULL, target TEXT NOT NULL,
			method TEXT NOT NULL, grant_id TEXT NOT NULL, request_id TEXT NOT NULL,
			result_code INTEGER NOT NULL, reason TEXT NOT NULL, occurred_at INTEGER NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("create host schema: %w", err)
		}
	}
	return nil
}

func migrateV2(tx *sql.Tx) error {
	statements := []string{
		`ALTER TABLE environments ADD COLUMN owner_kind INTEGER NOT NULL DEFAULT 1 CHECK(owner_kind IN (1, 2))`,
		`ALTER TABLE environments ADD COLUMN registration_id TEXT CHECK((owner_kind = 1 AND registration_id IS NULL) OR (owner_kind = 2 AND registration_id IS NOT NULL))`,
		`ALTER TABLE environments ADD COLUMN allocation_id TEXT CHECK((owner_kind = 1 AND allocation_id IS NULL) OR (owner_kind = 2 AND allocation_id IS NOT NULL))`,
		`ALTER TABLE environments ADD COLUMN allocation_state INTEGER CHECK((owner_kind = 1 AND allocation_state IS NULL) OR (owner_kind = 2 AND allocation_state IN (1, 2, 3, 4)))`,
		`CREATE UNIQUE INDEX environments_remote_allocation ON environments(registration_id, allocation_id) WHERE owner_kind = 2`,
		`CREATE TABLE remote_registrations (
			id TEXT PRIMARY KEY, endpoint TEXT NOT NULL, display_name TEXT NOT NULL, protocol_version TEXT NOT NULL,
			max_environments INTEGER NOT NULL CHECK(max_environments > 0),
			aggregate_cpu_millis INTEGER NOT NULL CHECK(aggregate_cpu_millis > 0),
			aggregate_memory_bytes INTEGER NOT NULL CHECK(aggregate_memory_bytes > 0),
			aggregate_pids INTEGER NOT NULL CHECK(aggregate_pids > 0),
			aggregate_disk_bytes INTEGER NOT NULL CHECK(aggregate_disk_bytes > 0),
			per_env_cpu_millis INTEGER NOT NULL CHECK(per_env_cpu_millis > 0),
			per_env_memory_bytes INTEGER NOT NULL CHECK(per_env_memory_bytes > 0),
			per_env_pids INTEGER NOT NULL CHECK(per_env_pids > 0),
			per_env_disk_bytes INTEGER NOT NULL CHECK(per_env_disk_bytes > 0),
			max_egress_mode INTEGER NOT NULL CHECK(max_egress_mode IN (1, 2)),
			max_lease_seconds INTEGER NOT NULL CHECK(max_lease_seconds > 0),
			disconnect_grace_seconds INTEGER NOT NULL CHECK(disconnect_grace_seconds > 0),
			stop_grace_seconds INTEGER NOT NULL CHECK(stop_grace_seconds > 0),
			revision INTEGER NOT NULL DEFAULT 1, state INTEGER NOT NULL CHECK(state IN (1, 2, 3)),
			connection_state INTEGER NOT NULL DEFAULT 1 CHECK(connection_state IN (1, 2, 3)),
			connection_instance TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE UNIQUE INDEX one_active_remote_registration ON remote_registrations((1)) WHERE state != 3`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("migrate host schema to v2: %w", err)
		}
	}
	return nil
}
