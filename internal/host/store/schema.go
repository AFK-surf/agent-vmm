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

// minSupportedSchema is the oldest existing database that this build converts.
// Schemas 26 through 34 inside the reported range were never released; Open
// refuses them as requiring offline conversion.
const minSupportedSchema = 18

// Codes of a database that this build refuses to open.
const (
	SchemaDataNewer              = "data_newer_than_host"
	SchemaDataRequiresConversion = "data_requires_conversion"
)

// SchemaError reports an existing database that this build does not open. Open
// returns it before it changes the database.
type SchemaError struct {
	Code       string
	DataSchema int
	message    string
}

func (err *SchemaError) Error() string { return err.message }

var errEncryptedApplianceStorage = errors.New("host database contains unsupported encrypted appliance storage; preserve the database, appliance secrets, and VM data")

// SchemaRange is an inclusive range of database schemas.
type SchemaRange struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

// SupportedSchema returns the existing database schemas that this build opens.
func SupportedSchema() SchemaRange { return SchemaRange{Min: minSupportedSchema, Max: schemaVersion} }

// StartFailureFile is the record that a Host writes next to its database when
// it refuses the database and exits without a launchd restart.
const StartFailureFile = "host-start-failure.json"

type StartFailure struct {
	Code            string      `json:"code"`
	DataSchema      int         `json:"dataSchema"`
	SupportedSchema SchemaRange `json:"supportedSchema"`
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
		return &SchemaError{Code: SchemaDataNewer, DataSchema: version,
			message: fmt.Sprintf("host database schema %d is newer than this Host supports (%d). Preserve the existing database", version, schemaVersion)}
	}
	if version != 0 && (version < minSupportedSchema || (version > 25 && version != schemaVersion)) {
		return &SchemaError{Code: SchemaDataRequiresConversion, DataSchema: version,
			message: fmt.Sprintf("host database schema %d requires offline conversion. Preserve the existing database", version)}
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
	if version < 24 {
		if err := migrateV24(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)", schemaVersion, time.Now().UnixNano()); err != nil {
			return err
		}
	}
	if version < 25 {
		if err := migrateV25(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)", schemaVersion, time.Now().UnixNano()); err != nil {
			return err
		}
	}
	if version < schemaVersion {
		if err := migrateV35(tx); err != nil {
			if errors.Is(err, errEncryptedApplianceStorage) {
				return &SchemaError{Code: SchemaDataRequiresConversion, DataSchema: version, message: err.Error()}
			}
			return err
		}
		if _, err := tx.Exec("INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)", schemaVersion, time.Now().UnixNano()); err != nil {
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

func migrateV35(tx *sql.Tx) error {
	for _, table := range []string{"appliance_installations", "appliance_volumes"} {
		var count int
		if err := tx.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errEncryptedApplianceStorage
		}
	}
	for _, statement := range []string{
		`ALTER TABLE appliance_grants ADD COLUMN lan_egress INTEGER NOT NULL DEFAULT 0 CHECK(lan_egress IN (0, 1))`,
		`DROP TABLE appliance_frontends`,
		`CREATE TABLE appliance_frontends (
			installation_id TEXT NOT NULL REFERENCES appliance_installations(id) ON DELETE CASCADE,
			name TEXT NOT NULL, transport INTEGER NOT NULL CHECK(transport IN (1, 2)),
			exposure INTEGER NOT NULL CHECK(exposure IN (1, 2, 3)), max_frame_bytes INTEGER NOT NULL,
			max_total_bytes INTEGER NOT NULL, deadline_seconds INTEGER NOT NULL, idle_seconds INTEGER NOT NULL,
			max_concurrent_streams INTEGER NOT NULL,
			PRIMARY KEY(installation_id, name), CHECK(exposure != 3 OR transport = 2))`,
		`DROP TABLE appliance_installations`,
		`CREATE TABLE appliance_installations (
				id TEXT PRIMARY KEY, product_id TEXT NOT NULL,
				artifact_id TEXT NOT NULL REFERENCES appliance_artifacts(artifact_id),
				desired_enabled INTEGER NOT NULL DEFAULT 0 CHECK(desired_enabled IN (0, 1)),
				state INTEGER NOT NULL CHECK(state BETWEEN 1 AND 6), revision INTEGER NOT NULL DEFAULT 1,
				registration_id TEXT UNIQUE REFERENCES remote_registrations(id),
				data_volume_id TEXT NOT NULL UNIQUE, data_volume_bytes INTEGER NOT NULL,
				bootstrap_id TEXT NOT NULL DEFAULT '', current_boot_id TEXT NOT NULL DEFAULT '',
				current_container_instance TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
			)`,
		`DROP TABLE appliance_volumes`,
		`ALTER TABLE appliance_grants ADD COLUMN host_tcp_endpoints TEXT NOT NULL DEFAULT '[]'`,
		`ALTER TABLE operations ADD COLUMN request_sha256 BLOB`,
		`ALTER TABLE operations ADD COLUMN receipt BLOB`,
		`ALTER TABLE access_routes ADD COLUMN source_installation_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE access_routes ADD COLUMN source_boot_id TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	if err := migrateActiveServiceAliases(tx); err != nil {
		return err
	}
	for _, statement := range []string{
		`ALTER TABLE service_imports ADD COLUMN destination_installation_id TEXT REFERENCES appliance_installations(id)`,
		`ALTER TABLE service_imports ADD COLUMN destination_frontend_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE access_routes ADD COLUMN destination_installation_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE access_routes ADD COLUMN destination_boot_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE access_routes ADD COLUMN destination_container_instance_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE access_routes ADD COLUMN destination_grant_revision INTEGER NOT NULL DEFAULT 0`,
		`CREATE TRIGGER service_import_destination_insert BEFORE INSERT ON service_imports WHEN
			 NOT ((NEW.destination_export_id!='' AND NEW.destination_installation_id IS NULL AND NEW.destination_frontend_name='') OR
			 (NEW.destination_export_id='' AND NEW.destination_installation_id IS NOT NULL AND NEW.destination_frontend_name='webhook' AND NEW.source_environment_id IS NOT NULL AND NEW.destination_port=80))
			 BEGIN SELECT RAISE(ABORT,'invalid service destination'); END`,
		`ALTER TABLE service_exports ADD COLUMN container_generation_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE service_imports ADD COLUMN source_container_generation_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE service_listeners ADD COLUMN local INTEGER NOT NULL DEFAULT 0 CHECK(local IN (0,1))`,
		`CREATE TABLE managed_containers (environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
			 container_id TEXT NOT NULL, container_generation_id TEXT NOT NULL, desired_running INTEGER NOT NULL CHECK(desired_running IN (0,1)),
			 revision INTEGER NOT NULL, PRIMARY KEY(environment_id,container_id))`,
		`ALTER TABLE service_listeners ADD COLUMN environment_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE service_listeners ADD COLUMN container_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE service_listeners ADD COLUMN target_port INTEGER NOT NULL DEFAULT 0`,
		`CREATE TABLE host_shares (
			id TEXT PRIMARY KEY,environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
			source_path TEXT NOT NULL,filesystem_uuid TEXT NOT NULL,file_id TEXT NOT NULL,
			read_only INTEGER NOT NULL CHECK(read_only IN(0,1)),revision INTEGER NOT NULL,state INTEGER NOT NULL,
			unavailable_reason TEXT NOT NULL,operation_id TEXT NOT NULL)`,
	} {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
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
