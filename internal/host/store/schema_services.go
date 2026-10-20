package store

import (
	"database/sql"
	"fmt"
)

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

func migrateActiveServiceAliases(tx *sql.Tx) error {
	rows, err := tx.Query(`SELECT sql FROM sqlite_master WHERE type='trigger' AND tbl_name='service_imports'`)
	if err != nil {
		return err
	}
	var triggers []string
	for rows.Next() {
		var statement string
		if err := rows.Scan(&statement); err != nil {
			rows.Close()
			return err
		}
		triggers = append(triggers, statement)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if _, err := tx.Exec(`PRAGMA legacy_alter_table=ON`); err != nil {
		return err
	}
	defer tx.Exec(`PRAGMA legacy_alter_table=OFF`)
	for _, statement := range []string{
		`CREATE TABLE service_imports_current (
 import_id TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,
 source_environment_id TEXT REFERENCES environments(id) ON DELETE CASCADE,
 source_container_id TEXT NOT NULL,source_container_instance_id TEXT NOT NULL,
 destination_device_id TEXT NOT NULL,destination_export_id TEXT NOT NULL,virtual_service_name TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 1,revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0,1)),
 created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,
 destination_port INTEGER NOT NULL DEFAULT 0 CHECK(destination_port BETWEEN 0 AND 65535),
 source_installation_id TEXT REFERENCES appliance_installations(id) ON DELETE CASCADE,
 source_namespace_id TEXT NOT NULL DEFAULT '',
 request_sha256 BLOB,revoke_request_id TEXT UNIQUE,revoke_request_sha256 BLOB,
 CHECK((source_environment_id IS NOT NULL) != (source_installation_id IS NOT NULL)))`,
		`INSERT INTO service_imports_current(import_id,request_id,source_environment_id,source_container_id,source_container_instance_id,destination_device_id,destination_export_id,virtual_service_name,revision,revoked,created_at,updated_at,destination_port)
 SELECT import_id,request_id,source_environment_id,source_container_id,source_container_instance_id,destination_device_id,destination_export_id,virtual_service_name,revision,revoked,created_at,updated_at,destination_port FROM service_imports`,
		`DROP TABLE service_imports`,
		`ALTER TABLE service_imports_current RENAME TO service_imports`,
		`CREATE UNIQUE INDEX service_imports_active_appliance_alias ON service_imports(source_installation_id,virtual_service_name) WHERE revoked=0`,
		`CREATE UNIQUE INDEX service_imports_active_alias ON service_imports(source_environment_id,virtual_service_name) WHERE revoked=0`,
	} {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	for _, statement := range triggers {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}

	return nil
}
