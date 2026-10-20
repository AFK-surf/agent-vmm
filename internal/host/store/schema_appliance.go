package store

import (
	"database/sql"
	"fmt"
)

func migrateV16(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE appliance_artifacts (
			artifact_id TEXT PRIMARY KEY, canonical_hash BLOB NOT NULL UNIQUE,
			image_digest TEXT NOT NULL, image_reference TEXT NOT NULL, platform TEXT NOT NULL,
			product_id TEXT NOT NULL, release TEXT NOT NULL,
			verified INTEGER NOT NULL CHECK(verified IN (0, 1)), manifest BLOB NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE appliance_installations (
			id TEXT PRIMARY KEY, product_id TEXT NOT NULL,
			artifact_id TEXT NOT NULL REFERENCES appliance_artifacts(artifact_id),
			desired_enabled INTEGER NOT NULL DEFAULT 0 CHECK(desired_enabled IN (0, 1)),
			state INTEGER NOT NULL CHECK(state BETWEEN 1 AND 6), revision INTEGER NOT NULL DEFAULT 1,
			registration_id TEXT UNIQUE REFERENCES remote_registrations(id),
			encrypted_volume_id TEXT NOT NULL UNIQUE, encrypted_volume_identity TEXT NOT NULL UNIQUE,
			data_volume_bytes INTEGER NOT NULL, bootstrap_id TEXT NOT NULL DEFAULT '', current_boot_id TEXT NOT NULL DEFAULT '',
			current_container_instance TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE appliance_grants (
			installation_id TEXT PRIMARY KEY REFERENCES appliance_installations(id) ON DELETE CASCADE,
			revision INTEGER NOT NULL, system_cpu_millis INTEGER NOT NULL, system_memory_bytes INTEGER NOT NULL,
			system_pids INTEGER NOT NULL, system_disk_bytes INTEGER NOT NULL, max_environments INTEGER NOT NULL,
			per_env_cpu_millis INTEGER NOT NULL, per_env_memory_bytes INTEGER NOT NULL,
			per_env_pids INTEGER NOT NULL, per_env_disk_bytes INTEGER NOT NULL,
			egress_mode INTEGER NOT NULL CHECK(egress_mode IN (1, 2)),
			max_egress_mode INTEGER NOT NULL CHECK(max_egress_mode IN (1, 2)),
			max_lease_seconds INTEGER NOT NULL, disconnect_grace_seconds INTEGER NOT NULL,
			dynamic_max_active_leases INTEGER NOT NULL, dynamic_max_active_leases_per_allocation INTEGER NOT NULL,
			dynamic_max_lease_seconds INTEGER NOT NULL, dynamic_max_concurrent_connections INTEGER NOT NULL,
			dynamic_max_concurrent_connections_per_lease INTEGER NOT NULL, dynamic_max_frame_bytes INTEGER NOT NULL,
			dynamic_max_connection_bytes INTEGER NOT NULL, dynamic_connection_deadline_seconds INTEGER NOT NULL,
			dynamic_connection_idle_seconds INTEGER NOT NULL, dynamic_max_use_count INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE appliance_frontends (
			installation_id TEXT NOT NULL REFERENCES appliance_installations(id) ON DELETE CASCADE,
			name TEXT NOT NULL, transport INTEGER NOT NULL CHECK(transport IN (1, 2)),
			exposure INTEGER NOT NULL CHECK(exposure IN (1, 2)), max_frame_bytes INTEGER NOT NULL,
			max_total_bytes INTEGER NOT NULL, deadline_seconds INTEGER NOT NULL, idle_seconds INTEGER NOT NULL,
			max_concurrent_streams INTEGER NOT NULL, max_use_count INTEGER NOT NULL,
			PRIMARY KEY(installation_id, name)
		)`,
		`CREATE TABLE appliance_volumes (
			installation_id TEXT PRIMARY KEY REFERENCES appliance_installations(id) ON DELETE CASCADE,
			volume_id TEXT NOT NULL UNIQUE, identity TEXT NOT NULL UNIQUE, bytes INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE appliance_trust_authorities (
			authority_id TEXT PRIMARY KEY REFERENCES trust_anchors(authority_id) ON DELETE CASCADE,
			installation_id TEXT NOT NULL UNIQUE REFERENCES appliance_installations(id) ON DELETE CASCADE,
			registration_id TEXT NOT NULL UNIQUE REFERENCES remote_registrations(id) ON DELETE CASCADE,
			boot_id TEXT NOT NULL, container_instance TEXT NOT NULL, config_revision INTEGER NOT NULL,
			state INTEGER NOT NULL CHECK(state IN (1, 2)), updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE appliance_operations (
			id TEXT PRIMARY KEY, installation_id TEXT NOT NULL, request_id TEXT NOT NULL UNIQUE,
			kind INTEGER NOT NULL, state INTEGER NOT NULL, expected_revision INTEGER NOT NULL,
			deadline_unix_millis INTEGER NOT NULL, stage TEXT NOT NULL, error_reason TEXT NOT NULL DEFAULT '',
			error_message TEXT NOT NULL DEFAULT '', operation_phase INTEGER NOT NULL DEFAULT 0,
			terminal_reason INTEGER NOT NULL DEFAULT 0, receipt BLOB NOT NULL DEFAULT X'',
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE revoked_appliance_authorities (
			authority_id TEXT PRIMARY KEY, installation_id TEXT NOT NULL,
			revoked_at INTEGER NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("migrate host schema to v16: %w", err)
		}
	}
	return nil
}

func migrateV20(tx *sql.Tx) error {
	if _, err := tx.Exec(`CREATE TABLE appliance_artifacts_v20 (
		artifact_id TEXT PRIMARY KEY, canonical_hash BLOB NOT NULL UNIQUE,
		image_digest TEXT NOT NULL, image_reference TEXT NOT NULL, platform TEXT NOT NULL,
		product_id TEXT NOT NULL, release TEXT NOT NULL, manifest BLOB NOT NULL,
		created_at INTEGER NOT NULL
	)`); err != nil {
		return fmt.Errorf("create artifact table without trust projection: %w", err)
	}
	if _, err := tx.Exec(`INSERT INTO appliance_artifacts_v20(
		artifact_id, canonical_hash, image_digest, image_reference, platform, product_id, release, manifest, created_at)
		SELECT artifact_id, canonical_hash, image_digest, image_reference, platform, product_id, release, manifest, created_at
		FROM appliance_artifacts`); err != nil {
		return fmt.Errorf("preserve appliance artifacts: %w", err)
	}
	if _, err := tx.Exec(`DROP TABLE appliance_artifacts`); err != nil {
		return fmt.Errorf("replace artifact table: %w", err)
	}
	if _, err := tx.Exec(`ALTER TABLE appliance_artifacts_v20 RENAME TO appliance_artifacts`); err != nil {
		return fmt.Errorf("activate artifact table without trust projection: %w", err)
	}
	return nil
}

func migrateV23(tx *sql.Tx) error {
	if _, err := tx.Exec(`ALTER TABLE appliance_volumes ADD COLUMN layout INTEGER NOT NULL DEFAULT 1 CHECK(layout IN (1, 2))`); err != nil {
		return fmt.Errorf("add appliance volume layout: %w", err)
	}
	return nil
}
