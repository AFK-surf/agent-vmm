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
	if version != 0 && version != 18 && version != 19 && version != 20 && version != 21 && version != 22 && version != schemaVersion {
		return fmt.Errorf("host database schema %d requires offline conversion. Preserve the existing database", version)
	}
	if version == 0 {
		for _, migration := range []func(*sql.Tx) error{migrateV1, migrateV2, migrateV3, migrateV4, migrateV5, migrateV6, migrateV9, migrateV10, migrateV16, migrateV19, migrateV20, migrateV21, migrateV22, migrateV23} {
			if err := migration(tx); err != nil {
				return err
			}
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", schemaVersion, time.Now().UnixNano()); err != nil {
			return err
		}
	} else if version == 18 {
		if err := migrateV18ToV19(tx); err != nil {
			return err
		}
		if err := migrateV20(tx); err != nil {
			return err
		}
		if err := migrateV21(tx); err != nil {
			return err
		}
		if err := migrateV22(tx); err != nil {
			return err
		}
		if err := migrateV23(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", schemaVersion, time.Now().UnixNano()); err != nil {
			return err
		}
	} else if version == 19 {
		if err := migrateV20(tx); err != nil {
			return err
		}
		if err := migrateV21(tx); err != nil {
			return err
		}
		if err := migrateV22(tx); err != nil {
			return err
		}
		if err := migrateV23(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", schemaVersion, time.Now().UnixNano()); err != nil {
			return err
		}
	} else if version == 20 {
		if err := migrateV21(tx); err != nil {
			return err
		}
		if err := migrateV22(tx); err != nil {
			return err
		}
		if err := migrateV23(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", schemaVersion, time.Now().UnixNano()); err != nil {
			return err
		}
	} else if version == 21 {
		if err := migrateV22(tx); err != nil {
			return err
		}
		if err := migrateV23(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", schemaVersion, time.Now().UnixNano()); err != nil {
			return err
		}
	} else if version == 22 {
		if err := migrateV23(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", schemaVersion, time.Now().UnixNano()); err != nil {
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

func migrateV23(tx *sql.Tx) error {
	if _, err := tx.Exec(`ALTER TABLE appliance_volumes ADD COLUMN layout INTEGER NOT NULL DEFAULT 1 CHECK(layout IN (1, 2))`); err != nil {
		return fmt.Errorf("add appliance volume layout: %w", err)
	}
	return nil
}

func migrateV22(tx *sql.Tx) error {
	if _, err := tx.Exec(`DROP TABLE resource_reservations`); err != nil {
		return fmt.Errorf("remove compute reservation projection: %w", err)
	}
	for _, column := range []string{"max_environments", "per_env_cpu_millis", "per_env_memory_bytes"} {
		if _, err := tx.Exec(`ALTER TABLE remote_registrations DROP COLUMN ` + column); err != nil {
			return fmt.Errorf("remove remote compute projection %s: %w", column, err)
		}
	}
	for _, column := range []string{"system_cpu_millis", "system_memory_bytes", "max_environments", "per_env_cpu_millis", "per_env_memory_bytes"} {
		if _, err := tx.Exec(`ALTER TABLE appliance_grants DROP COLUMN ` + column); err != nil {
			return fmt.Errorf("remove appliance compute projection %s: %w", column, err)
		}
	}
	return nil
}

func migrateV21(tx *sql.Tx) error {
	if _, err := tx.Exec(`UPDATE environments SET allocation_state=1, policy_revision=policy_revision+1,
		updated_at=? WHERE owner_kind=2 AND allocation_state=2`, time.Now().UnixNano()); err != nil {
		return fmt.Errorf("restore leased allocations to durable ownership: %w", err)
	}
	if _, err := tx.Exec(`DROP TABLE remote_lease_projections`); err != nil {
		return fmt.Errorf("remove allocation lease projections: %w", err)
	}
	for _, column := range []string{"max_lease_seconds", "disconnect_grace_seconds"} {
		if _, err := tx.Exec(`ALTER TABLE remote_registrations DROP COLUMN ` + column); err != nil {
			return fmt.Errorf("remove remote allocation lease policy %s: %w", column, err)
		}
		if _, err := tx.Exec(`ALTER TABLE appliance_grants DROP COLUMN ` + column); err != nil {
			return fmt.Errorf("remove appliance allocation lease policy %s: %w", column, err)
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

func migrateV19(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE environments ADD COLUMN allocation_generation INTEGER NOT NULL DEFAULT 0`)
	return err
}

func migrateV18ToV19(tx *sql.Tx) error {
	if err := migrateV19(tx); err != nil {
		return fmt.Errorf("add allocation generation: %w", err)
	}
	if _, err := tx.Exec(`UPDATE environments SET allocation_generation=COALESCE((
		SELECT generation FROM workload_reservations_v2 AS reservation
		WHERE reservation.environment_id=environments.id
		  AND reservation.registration_id=environments.registration_id
		  AND reservation.allocation_id=environments.allocation_id
	), 0) WHERE owner_kind=2`); err != nil {
		return fmt.Errorf("convert allocation generation: %w", err)
	}
	var missing int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM environments
		WHERE owner_kind=2 AND allocation_generation=0`).Scan(&missing); err != nil {
		return fmt.Errorf("verify allocation generation conversion: %w", err)
	}
	if missing != 0 {
		return fmt.Errorf("convert allocation generation: %d remote environments lack an exact reservation", missing)
	}
	if _, err := tx.Exec(`DROP TRIGGER IF EXISTS remote_reservation_delete`); err != nil {
		return fmt.Errorf("remove obsolete allocation reservation trigger: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM resource_reservations WHERE owner_kind NOT IN (1, 2) OR resource_kind NOT IN (1, 2)`); err != nil {
		return fmt.Errorf("remove obsolete allocation reservations: %w", err)
	}
	if _, err := tx.Exec(`DROP TABLE workload_reservations_v2`); err != nil {
		return fmt.Errorf("remove obsolete workload reservations: %w", err)
	}
	for _, column := range []string{"aggregate_cpu_millis", "aggregate_memory_bytes", "aggregate_pids", "aggregate_disk_bytes"} {
		if _, err := tx.Exec(`ALTER TABLE remote_registrations DROP COLUMN ` + column); err != nil {
			return fmt.Errorf("remove remote aggregate limit %s: %w", column, err)
		}
		if _, err := tx.Exec(`ALTER TABLE appliance_grants DROP COLUMN ` + column); err != nil {
			return fmt.Errorf("remove appliance aggregate limit %s: %w", column, err)
		}
	}
	if _, err := tx.Exec(`CREATE TABLE resource_reservations_v19 (
		id TEXT PRIMARY KEY, owner_kind INTEGER NOT NULL CHECK(owner_kind IN (1, 2)), owner_id TEXT NOT NULL,
		resource_kind INTEGER NOT NULL CHECK(resource_kind IN (1, 2)), resource_id TEXT NOT NULL,
		cpu_millis INTEGER NOT NULL, memory_bytes INTEGER NOT NULL, pids INTEGER NOT NULL, disk_bytes INTEGER NOT NULL,
		retained INTEGER NOT NULL DEFAULT 0 CHECK(retained IN (0, 1)), created_at INTEGER NOT NULL,
		UNIQUE(owner_kind, owner_id, resource_kind, resource_id)
	)`); err != nil {
		return fmt.Errorf("create fixed resource reservation table: %w", err)
	}
	if _, err := tx.Exec(`INSERT INTO resource_reservations_v19 SELECT * FROM resource_reservations`); err != nil {
		return fmt.Errorf("preserve fixed resource reservations: %w", err)
	}
	if _, err := tx.Exec(`DROP TABLE resource_reservations`); err != nil {
		return fmt.Errorf("replace obsolete resource reservation table: %w", err)
	}
	if _, err := tx.Exec(`ALTER TABLE resource_reservations_v19 RENAME TO resource_reservations`); err != nil {
		return fmt.Errorf("activate fixed resource reservation table: %w", err)
	}
	return nil
}

// migrateV18Legacy creates the immediately previous schema for conversion
// tests. Production initialization does not create this retired table.
func migrateV18Legacy(tx *sql.Tx) error {
	statements := []string{
		`DROP TABLE resource_reservations`,
		`CREATE TABLE resource_reservations (
			id TEXT PRIMARY KEY, owner_kind INTEGER NOT NULL CHECK(owner_kind IN (1, 2, 3)), owner_id TEXT NOT NULL,
			resource_kind INTEGER NOT NULL CHECK(resource_kind IN (1, 2, 3)), resource_id TEXT NOT NULL,
			cpu_millis INTEGER NOT NULL, memory_bytes INTEGER NOT NULL, pids INTEGER NOT NULL, disk_bytes INTEGER NOT NULL,
			retained INTEGER NOT NULL DEFAULT 0 CHECK(retained IN (0, 1)), created_at INTEGER NOT NULL,
			UNIQUE(owner_kind, owner_id, resource_kind, resource_id)
		)`,
		`CREATE TABLE workload_reservations_v2 (
		allocation_id TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation > 0),
		registration_id TEXT NOT NULL REFERENCES remote_registrations(id) ON DELETE CASCADE,
		environment_id TEXT NOT NULL UNIQUE REFERENCES environments(id) ON DELETE CASCADE,
		cpu_guarantee_millis INTEGER NOT NULL, cpu_max_millis INTEGER NOT NULL, cpu_weight INTEGER NOT NULL,
		memory_high_bytes INTEGER NOT NULL, memory_max_bytes INTEGER NOT NULL, pid_max INTEGER NOT NULL,
		writable_quota_bytes INTEGER NOT NULL, phase INTEGER NOT NULL CHECK(phase BETWEEN 1 AND 5),
		guest_receipt BLOB NOT NULL DEFAULT X'', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
		PRIMARY KEY(allocation_id, generation), UNIQUE(registration_id, allocation_id)
	)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

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

func migrateV10(tx *sql.Tx) error {
	statements := []string{
		`ALTER TABLE remote_registrations ADD COLUMN owner_kind INTEGER NOT NULL DEFAULT 1 CHECK(owner_kind IN (1, 2))`,
		`ALTER TABLE remote_registrations ADD COLUMN installation_id TEXT`,
		`ALTER TABLE remote_registrations ADD COLUMN inventory_revision INTEGER NOT NULL DEFAULT 1 CHECK(inventory_revision > 0)`,
		`ALTER TABLE remote_registrations ADD COLUMN pool_policy_revision INTEGER NOT NULL DEFAULT 1 CHECK(pool_policy_revision > 0)`,
		`CREATE UNIQUE INDEX remote_registration_installation ON remote_registrations(installation_id) WHERE installation_id IS NOT NULL`,
		`CREATE TRIGGER remote_registration_owner_insert BEFORE INSERT ON remote_registrations
			WHEN (NEW.owner_kind = 1 AND NEW.installation_id IS NOT NULL) OR (NEW.owner_kind = 2 AND NEW.installation_id IS NULL)
			BEGIN SELECT RAISE(ABORT, 'registration owner binding is invalid'); END`,
		`CREATE TRIGGER remote_registration_owner_update BEFORE UPDATE OF owner_kind, installation_id ON remote_registrations
			WHEN (NEW.owner_kind = 1 AND NEW.installation_id IS NOT NULL) OR (NEW.owner_kind = 2 AND NEW.installation_id IS NULL)
			BEGIN SELECT RAISE(ABORT, 'registration owner binding is invalid'); END`,
		`CREATE TABLE resource_reservations (
			id TEXT PRIMARY KEY, owner_kind INTEGER NOT NULL CHECK(owner_kind IN (1, 2)), owner_id TEXT NOT NULL,
			resource_kind INTEGER NOT NULL CHECK(resource_kind IN (1, 2)), resource_id TEXT NOT NULL,
			cpu_millis INTEGER NOT NULL, memory_bytes INTEGER NOT NULL, pids INTEGER NOT NULL, disk_bytes INTEGER NOT NULL,
			retained INTEGER NOT NULL DEFAULT 0 CHECK(retained IN (0, 1)), created_at INTEGER NOT NULL,
			UNIQUE(owner_kind, owner_id, resource_kind, resource_id)
		)`,
		`CREATE TABLE remote_lease_projections (
			lease_id TEXT PRIMARY KEY, registration_id TEXT NOT NULL REFERENCES remote_registrations(id) ON DELETE CASCADE,
			allocation_id TEXT NOT NULL, allocation_generation INTEGER NOT NULL CHECK(allocation_generation > 0),
			environment_id TEXT NOT NULL, workload_ref TEXT NOT NULL,
			generation INTEGER NOT NULL, expires_at INTEGER NOT NULL, state INTEGER NOT NULL,
			UNIQUE(registration_id, allocation_id)
		)`,
		`CREATE TRIGGER remote_inventory_insert AFTER INSERT ON environments WHEN NEW.owner_kind = 2
			BEGIN UPDATE remote_registrations SET inventory_revision=inventory_revision+1, updated_at=NEW.updated_at WHERE id=NEW.registration_id; END`,
		`CREATE TRIGGER remote_inventory_update AFTER UPDATE OF cpu_millis, memory_bytes, pids, disk_bytes, egress_mode, policy_revision, allocation_state ON environments WHEN NEW.owner_kind = 2
			BEGIN UPDATE remote_registrations SET inventory_revision=inventory_revision+1, updated_at=NEW.updated_at WHERE id=NEW.registration_id; END`,
		`CREATE TRIGGER remote_inventory_delete AFTER DELETE ON environments WHEN OLD.owner_kind = 2
			BEGIN UPDATE remote_registrations SET inventory_revision=inventory_revision+1, updated_at=strftime('%s','now')*1000000000 WHERE id=OLD.registration_id; END`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("migrate host schema to v10: %w", err)
		}
	}
	return nil
}

func migrateV9(tx *sql.Tx) error {
	statements := []string{
		`ALTER TABLE service_imports ADD COLUMN destination_port INTEGER NOT NULL DEFAULT 0 CHECK(destination_port BETWEEN 0 AND 65535)`,
		`CREATE TABLE personal_route_issuances (
			request_id TEXT PRIMARY KEY, mesh_id TEXT NOT NULL, source_device_id TEXT NOT NULL,
			destination_export_id TEXT NOT NULL, expires_at INTEGER NOT NULL, response BLOB NOT NULL, created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE service_listeners (
			listener_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, mesh_id TEXT NOT NULL,
			destination_device_id TEXT NOT NULL, destination_export_id TEXT NOT NULL,
			exposure INTEGER NOT NULL CHECK(exposure IN (1, 2)), listen_address TEXT NOT NULL,
			route_id TEXT NOT NULL UNIQUE, capability_id TEXT NOT NULL, capability BLOB NOT NULL,
			endpoint BLOB NOT NULL, expires_at INTEGER NOT NULL, revision INTEGER NOT NULL,
			revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0, 1)), created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("create current route schema: %w", err)
		}
	}
	return nil
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
			custody TEXT NOT NULL CHECK(custody IN ('secure_enclave', 'keychain', 'file', 'test')),
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
		`CREATE INDEX audit_retention ON audit(occurred_at, sequence)`,
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
			trust_bundle BLOB NOT NULL DEFAULT X'', scope_digest TEXT NOT NULL DEFAULT '',
			source_policy_revision INTEGER NOT NULL DEFAULT 0 CHECK(source_policy_revision >= 0),
			max_environments INTEGER NOT NULL CHECK(max_environments > 0),
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
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("migrate host schema to v2: %w", err)
		}
	}
	return nil
}
