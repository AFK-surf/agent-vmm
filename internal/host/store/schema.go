package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (store *Store) migrate(ctx context.Context) error {
	if _, err := store.db.ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		return err
	}
	foreignKeysRestored := false
	defer func() {
		if !foreignKeysRestored {
			_, _ = store.db.ExecContext(context.Background(), "PRAGMA foreign_keys=ON")
		}
	}()
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
		version = 2
	}
	if version == 2 {
		if err := migrateV3(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (3, ?)", time.Now().UnixNano()); err != nil {
			return err
		}
		version = 3
	}
	if version == 3 {
		if err := migrateV4(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (4, ?)", time.Now().UnixNano()); err != nil {
			return err
		}
		version = 4
	}
	if version == 4 {
		if err := migrateV5(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (5, ?)", time.Now().UnixNano()); err != nil {
			return err
		}
		version = 5
	}
	if version == 5 {
		if err := migrateV6(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (6, ?)", time.Now().UnixNano()); err != nil {
			return err
		}
		version = 6
	}
	if version == 6 {
		if _, err := tx.Exec(`ALTER TABLE service_imports ADD COLUMN destination_port INTEGER NOT NULL DEFAULT 0 CHECK(destination_port BETWEEN 0 AND 65535)`); err != nil {
			return fmt.Errorf("migrate host schema to v7: %w", err)
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (7, ?)", time.Now().UnixNano()); err != nil {
			return err
		}
		version = 7
	}
	if version == 7 {
		if _, err := tx.Exec(`CREATE TABLE personal_route_issuances (
			request_id TEXT PRIMARY KEY, mesh_id TEXT NOT NULL, source_device_id TEXT NOT NULL,
			destination_export_id TEXT NOT NULL, expires_at INTEGER NOT NULL, response BLOB NOT NULL, created_at INTEGER NOT NULL
		)`); err != nil {
			return fmt.Errorf("migrate host schema to v8: %w", err)
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (8, ?)", time.Now().UnixNano()); err != nil {
			return err
		}
		version = 8
	}
	if version == 8 {
		if _, err := tx.Exec(`CREATE TABLE service_listeners (
			listener_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, mesh_id TEXT NOT NULL,
			destination_device_id TEXT NOT NULL, destination_export_id TEXT NOT NULL,
			exposure INTEGER NOT NULL CHECK(exposure IN (1, 2)), listen_address TEXT NOT NULL,
			route_id TEXT NOT NULL UNIQUE, capability_id TEXT NOT NULL, capability BLOB NOT NULL,
			endpoint BLOB NOT NULL, expires_at INTEGER NOT NULL, revision INTEGER NOT NULL,
			revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0, 1)), created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		)`); err != nil {
			return fmt.Errorf("migrate host schema to v9: %w", err)
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (9, ?)", time.Now().UnixNano()); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if _, err := store.db.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		return err
	}
	foreignKeysRestored = true
	rows, err := store.db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("host database contains foreign key violations after schema migration")
	}
	return rows.Err()
}

func migrateV6(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE service_exports (
			export_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE,
			environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
			container_id TEXT NOT NULL, container_instance_id TEXT NOT NULL, port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535),
			protocol INTEGER NOT NULL CHECK(protocol IN (1, 2)), revision INTEGER NOT NULL DEFAULT 1,
			revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0, 1)), created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE INDEX service_exports_environment ON service_exports(environment_id, revoked, export_id)`,
		`CREATE TABLE service_imports (
			import_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE,
			source_environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
			source_container_id TEXT NOT NULL, source_container_instance_id TEXT NOT NULL,
			destination_device_id TEXT NOT NULL, destination_export_id TEXT NOT NULL, virtual_service_name TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 1, revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0, 1)),
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
			UNIQUE(source_environment_id, virtual_service_name)
		)`,
		`CREATE TABLE access_routes (
			route_id TEXT PRIMARY KEY, import_id TEXT NOT NULL UNIQUE REFERENCES service_imports(import_id) ON DELETE CASCADE,
			route_class INTEGER NOT NULL CHECK(route_class BETWEEN 1 AND 5), source_device_id TEXT NOT NULL,
			source_allocation_id TEXT, destination_device_id TEXT NOT NULL, destination_export_id TEXT NOT NULL,
			state INTEGER NOT NULL CHECK(state BETWEEN 1 AND 5), revision INTEGER NOT NULL, generation INTEGER NOT NULL,
			expires_at INTEGER NOT NULL, source_environment_id TEXT NOT NULL, source_container_instance_id TEXT NOT NULL,
			policy_revision INTEGER NOT NULL, capability BLOB NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE route_leases (
			lease_id TEXT PRIMARY KEY, route_id TEXT NOT NULL UNIQUE REFERENCES access_routes(route_id) ON DELETE CASCADE,
			route_revision INTEGER NOT NULL, generation INTEGER NOT NULL, expires_at INTEGER NOT NULL,
			capability BLOB NOT NULL, created_at INTEGER NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("migrate host schema to v6: %w", err)
		}
	}
	return nil
}

func migrateV5(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE device_identity (
			id INTEGER PRIMARY KEY CHECK(id = 1), device_id TEXT NOT NULL UNIQUE,
			root_public_key BLOB NOT NULL, root_key_revision INTEGER NOT NULL CHECK(root_key_revision > 0),
			custody TEXT NOT NULL CHECK(custody IN ('secure_enclave', 'keychain', 'test')),
			state INTEGER NOT NULL CHECK(state IN (1, 2)), endpoint_generation INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE device_root_rotations (
			new_revision INTEGER PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, rotation BLOB NOT NULL, created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE device_repairs (
			request_id TEXT PRIMARY KEY, identity BLOB NOT NULL, repaired_at INTEGER NOT NULL
		)`,
		`CREATE TABLE trust_anchors (
			authority_id TEXT PRIMARY KEY, owner_kind INTEGER NOT NULL CHECK(owner_kind IN (1, 2)),
			revision INTEGER NOT NULL CHECK(revision > 0), expires_at INTEGER NOT NULL,
			anchor BLOB NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE membership_credentials (
			authority_id TEXT NOT NULL, subject_device_id TEXT NOT NULL, policy_revision INTEGER NOT NULL,
			expires_at INTEGER NOT NULL, credential BLOB NOT NULL, updated_at INTEGER NOT NULL,
			PRIMARY KEY(authority_id, subject_device_id)
		)`,
		`CREATE TABLE mesh_descriptors (
			mesh_id TEXT PRIMARY KEY, owner_kind INTEGER NOT NULL CHECK(owner_kind = 2),
			descriptor BLOB NOT NULL, created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE mesh_snapshots (
			mesh_id TEXT PRIMARY KEY, owner_kind INTEGER NOT NULL CHECK(owner_kind = 2),
			revision INTEGER NOT NULL CHECK(revision > 0), policy_epoch INTEGER NOT NULL CHECK(policy_epoch > 0),
			expires_at INTEGER NOT NULL, snapshot BLOB NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE mesh_tombstones (
			mesh_id TEXT NOT NULL, device_id TEXT NOT NULL, root_key_revision INTEGER NOT NULL,
			revoked_at_revision INTEGER NOT NULL, policy_epoch INTEGER NOT NULL, operation_digest BLOB NOT NULL,
			PRIMARY KEY(mesh_id, device_id)
		)`,
		`CREATE TABLE endpoint_observations (
			device_id TEXT PRIMARY KEY, root_key_revision INTEGER NOT NULL, endpoint_generation INTEGER NOT NULL,
			expires_at INTEGER NOT NULL, observation BLOB NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE route_denials (
			capability_id TEXT PRIMARY KEY, denied_at INTEGER NOT NULL, reason TEXT NOT NULL
		)`,
		`CREATE TABLE mesh_member_denials (
			mesh_id TEXT NOT NULL, device_id TEXT NOT NULL, root_key_revision INTEGER NOT NULL,
			operation_id TEXT NOT NULL UNIQUE, denied_at INTEGER NOT NULL, PRIMARY KEY(mesh_id, device_id)
		)`,
		`CREATE TABLE registry_operations (
			sequence INTEGER PRIMARY KEY AUTOINCREMENT, operation_id TEXT NOT NULL UNIQUE,
			mesh_id TEXT NOT NULL, expected_revision INTEGER NOT NULL, kind INTEGER NOT NULL,
			state INTEGER NOT NULL CHECK(state IN (1, 2)), operation BLOB NOT NULL,
			created_at INTEGER NOT NULL, completed_at INTEGER
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("migrate host schema to v5: %w", err)
		}
	}
	return nil
}

func migrateV4(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE agent_integrations_v4 (
			id TEXT PRIMARY KEY, adapter_id TEXT NOT NULL, display_name TEXT NOT NULL,
			scope INTEGER NOT NULL CHECK(scope IN (1, 2, 3)), config_target TEXT NOT NULL,
			environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE RESTRICT,
			desired_state INTEGER NOT NULL CHECK(desired_state IN (1, 2)),
			config_state INTEGER NOT NULL CHECK(config_state BETWEEN 1 AND 8),
			config_fingerprint TEXT NOT NULL DEFAULT '', revision INTEGER NOT NULL DEFAULT 1,
			last_error_code TEXT NOT NULL DEFAULT '', last_error_message TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, last_verified_at INTEGER,
			prepare_request_id TEXT NOT NULL UNIQUE
		)`,
		`INSERT INTO agent_integrations_v4 SELECT * FROM agent_integrations`,
		`DROP TABLE agent_integrations`,
		`ALTER TABLE agent_integrations_v4 RENAME TO agent_integrations`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("migrate host schema to v4: %w", err)
		}
	}
	return nil
}

func migrateV3(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE agent_integrations (
			id TEXT PRIMARY KEY, adapter_id TEXT NOT NULL, display_name TEXT NOT NULL,
			scope INTEGER NOT NULL CHECK(scope IN (1, 2, 3)), config_target TEXT NOT NULL,
			environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE RESTRICT,
			desired_state INTEGER NOT NULL CHECK(desired_state IN (1, 2)),
			config_state INTEGER NOT NULL CHECK(config_state BETWEEN 1 AND 7),
			config_fingerprint TEXT NOT NULL DEFAULT '', revision INTEGER NOT NULL DEFAULT 1,
			last_error_code TEXT NOT NULL DEFAULT '', last_error_message TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, last_verified_at INTEGER,
			prepare_request_id TEXT NOT NULL UNIQUE
		)`,
		`ALTER TABLE sessions ADD COLUMN integration_id TEXT REFERENCES agent_integrations(id)`,
		`ALTER TABLE sessions ADD COLUMN origin INTEGER NOT NULL DEFAULT 0 CHECK(origin IN (0, 1, 2))`,
		`ALTER TABLE sessions ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE sessions ADD COLUMN connected_at INTEGER`,
		`ALTER TABLE sessions ADD COLUMN last_activity_at INTEGER`,
		`ALTER TABLE sessions ADD COLUMN closed_at INTEGER`,
		`ALTER TABLE sessions ADD COLUMN terminal_reason TEXT NOT NULL DEFAULT ''`,
		`CREATE INDEX sessions_integration ON sessions(integration_id, created_at)`,
		`ALTER TABLE remote_registrations ADD COLUMN enrollment_state INTEGER NOT NULL DEFAULT 3 CHECK(enrollment_state IN (1, 2, 3, 4))`,
		`ALTER TABLE remote_registrations ADD COLUMN enrollment_error_code TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE remote_registrations ADD COLUMN enrollment_error_message TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE remote_registrations ADD COLUMN connector_phase INTEGER NOT NULL DEFAULT 1 CHECK(connector_phase BETWEEN 1 AND 7)`,
		`ALTER TABLE remote_registrations ADD COLUMN connector_release TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE remote_registrations ADD COLUMN connector_observed_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE remote_registrations ADD COLUMN last_connected_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE remote_registrations ADD COLUMN disconnected_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE remote_registrations ADD COLUMN next_retry_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE remote_registrations ADD COLUMN reconnect_attempt INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE remote_registrations ADD COLUMN connector_failure_code TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE remote_registrations ADD COLUMN connector_failure_message TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE remote_registrations ADD COLUMN enrollment_request_id TEXT NOT NULL DEFAULT ''`,
		`CREATE UNIQUE INDEX remote_registration_enrollment_request ON remote_registrations(enrollment_request_id) WHERE enrollment_request_id != ''`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("migrate host schema to v3: %w", err)
		}
	}
	return nil
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
