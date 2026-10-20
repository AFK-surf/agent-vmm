package store

import (
	"database/sql"
	"fmt"
)

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
